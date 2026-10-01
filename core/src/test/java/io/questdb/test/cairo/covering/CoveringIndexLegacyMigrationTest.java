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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.idx.PostingIndexChainHeader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Dual-format compatibility and format-1 migration for the covering cover-end
 * footer de-alias.
 * <p>
 * {@code PostingIndexWriter.FORCE_LEGACY_COVERING_FORMAT} synthesises a
 * pre-9.4.x on-disk covering index (format 0, aliased trailing footer). This
 * test asserts:
 * <ul>
 *   <li>the de-aliased build READS a legacy format-0 covering index correctly
 *       (covered scans match the base column) — the on-disk backward-compat
 *       guarantee;</li>
 *   <li>continuing to write MIGRATES that legacy head to the de-aliased format 1
 *       — either by {@code publishToChain}'s copy-on-write or by a covering
 *       reseal appending a fresh format-1 entry — after which covered reads
 *       remain exact. Without the migration a legacy covering head would be
 *       extended in place and re-expose the concurrent covered-read OOB.</li>
 * </ul>
 */
public class CoveringIndexLegacyMigrationTest extends AbstractCairoTest {

    @After
    public void disableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
    }

    @Before
    public void enableCoveringCounters() {
        // The migration leaves no trace in query results, so the only way to
        // observe that it ran is the writer's counters.
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
    }

    @Test
    public void testLegacyFormat0ReadCompatAndMigrationOnWrite() throws Exception {
        assertMemoryLeak(() -> {
            // Covering (POSTING INCLUDE) table + a non-covering control fed the
            // same rows. BYPASS WAL so releaseAllWriters + the next write reopens
            // the partition, which is what drives the legacy head's migration.
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (price), price DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE ctl (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, price DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            // Build LEGACY (format-0, aliased) covering entries on disk. Many
            // small commits accumulate several gens so the covering footer is a
            // real multi-gen trailing footer.
            PostingIndexWriter.FORCE_LEGACY_COVERING_FORMAT = true;
            try {
                for (int b = 0; b < 8; b++) {
                    final String sel = " SELECT dateadd('s', (" + (b * 40) + " + x)::INT, '2024-03-01T00:00:00Z'::TIMESTAMP),"
                            + " 'S' || ((" + (b * 40) + " + x) % 5), (" + (b * 40) + " + x)::DOUBLE FROM long_sequence(40)";
                    execute("INSERT INTO t" + sel);
                    execute("INSERT INTO ctl" + sel);
                }
            } finally {
                PostingIndexWriter.FORCE_LEGACY_COVERING_FORMAT = false;
            }

            // (1) A legacy format-0 covering index must READ correctly under the
            // de-aliased build: covered scans == the non-covering control.
            assertCoveredMatchesControl();

            // (2) Reopen and keep writing with the de-aliased build active. Covered
            // reads must stay exact vs the control across the format-0 -> format-1
            // transition, with no OOB / corruption.
            //
            // Counters are deliberately NOT reset here. FORCE_LEGACY_COVERING_FORMAT
            // only forces NEW entries to format 0; it does not stop publishToChain
            // migrating a legacy head on the next same-seal extend. So the migration
            // fires during phase (1) as well, and zeroing the counters here would
            // hide the very event this test exists to observe.
            engine.releaseAllWriters();
            for (int b = 10; b < 16; b++) {
                final String sel = " SELECT dateadd('s', (" + (b * 40) + " + x)::INT, '2024-03-01T00:00:00Z'::TIMESTAMP),"
                        + " 'S' || ((" + (b * 40) + " + x) % 5), (" + (b * 40) + " + x)::DOUBLE FROM long_sequence(40)";
                execute("INSERT INTO t" + sel);
                execute("INSERT INTO ctl" + sel);
            }
            assertCoveredMatchesControl();

            // Correct covered reads alone do not prove anything about the layout --
            // an un-migrated format-0 head reads correctly too. Pin the migration
            // itself, so dropping it fails here instead of silently leaving legacy
            // heads to be extended in place and re-exposing the aliased-footer OOB.
            Assert.assertTrue(
                    "legacy format-0 covering head must be COW-migrated to format 1",
                    PostingIndexWriter.COVERING_COW_MIGRATE_COUNT.get() > 0
            );

            // And pin the on-disk consequence: FORMAT_VERSION is raised to V3 only
            // when a de-aliased covering entry lands, so an older build is refused
            // rather than left to misread the new layout.
            engine.releaseAllWriters();
            Assert.assertEquals(
                    "a de-aliased covering entry must raise the .pk format version",
                    PostingIndexUtils.V3_FORMAT_VERSION,
                    readPkFormatVersion("t", "2024-03-01", "sym")
            );
        });
    }

    @Test
    public void testUnsupportedPkFormatVersionIsRejectedByQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (price), price DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t SELECT dateadd('s', x::INT, '2024-03-01T00:00:00Z'::TIMESTAMP),"
                    + " 'S' || (x % 5), x::DOUBLE FROM long_sequence(200)");

            // Covered reads work before the file is tampered with.
            assertQuery("SELECT count(*) FROM t WHERE sym = 'S1'").noRandomAccess().expectSize().returns("count\n40\n");

            // A .pk carrying a version this build does not know -- what a reader
            // would meet after a downgrade, a forward-dated file, or corruption.
            // Release first so the next query re-opens rather than serving a
            // cached reader, and stamp both header pages so whichever the
            // seqlock picks carries the bad value.
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            stampPkFormatVersion("t", "2024-03-01", "sym", 99L);

            // It must fail loudly. Silently misreading the entry layout is the
            // outcome the version gate exists to prevent.
            try (RecordCursorFactory factory = select("SELECT ts, sym, price FROM t WHERE sym = 'S1'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                }
                Assert.fail("query must reject an unsupported posting index version");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Unsupported Posting index version");
            }
        });
    }

    // FORMAT_VERSION off the active header page of a live table's .pk, read the
    // same way a concurrent reader would.
    private long readPkFormatVersion(String tableName, String partition, String columnName) {
        return withPk(tableName, partition, columnName, pk -> {
            PostingIndexChainHeader.Snapshot snap = new PostingIndexChainHeader.Snapshot();
            Assert.assertTrue("posting chain header must be readable", PostingIndexChainHeader.readUnderSeqlock(pk, snap));
            return snap.formatVersion;
        });
    }

    // Overwrite FORMAT_VERSION on BOTH header pages of a live table's .pk.
    private void stampPkFormatVersion(String tableName, String partition, String columnName, long version) {
        withPk(tableName, partition, columnName, pk -> {
            pk.putLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_FORMAT_VERSION, version);
            pk.putLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_FORMAT_VERSION, version);
            return 0L;
        });
    }

    private long withPk(String tableName, String partition, String columnName, PkAction action) {
        final TableToken token = engine.verifyTableName(tableName);
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token).concat(partition).slash();
            final int plen = path.size();
            final long len = ff.length(PostingIndexUtils.keyFileName(path.trimTo(plen), columnName, COLUMN_NAME_TXN_NONE));
            Assert.assertTrue("posting key file must exist", len > 0);
            try (MemoryCMARWImpl pk = new MemoryCMARWImpl(
                    ff,
                    PostingIndexUtils.keyFileName(path.trimTo(plen), columnName, COLUMN_NAME_TXN_NONE),
                    ff.getPageSize(),
                    len,
                    MemoryTag.MMAP_DEFAULT,
                    0
            )) {
                return action.apply(pk);
            }
        }
    }

    @FunctionalInterface
    private interface PkAction {
        long apply(MemoryCMARWImpl pk);
    }

    private void assertCoveredMatchesControl() throws Exception {
        for (int s = 0; s < 5; s++) {
            final String sym = "S" + s;
            assertSqlCursors(
                    "SELECT ts, sym, price FROM ctl WHERE sym = '" + sym + "' ORDER BY price",
                    "SELECT ts, sym, price FROM t WHERE sym = '" + sym + "' ORDER BY price"
            );
        }
        assertSqlCursors(
                "SELECT sym, sum(price), count(price), count(*), min(price), max(price) FROM ctl ORDER BY sym",
                "SELECT sym, sum(price), count(price), count(*), min(price), max(price) FROM t ORDER BY sym"
        );
    }
}
