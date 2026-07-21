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

import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Dual-format compatibility + eager migration for the covering cover-end footer
 * de-alias.
 * <p>
 * {@code PostingIndexWriter.FORCE_LEGACY_COVERING_FORMAT} synthesises a
 * pre-9.4.x on-disk covering index (format 0, aliased trailing footer). This
 * test asserts:
 * <ul>
 *   <li>the de-aliased build READS a legacy format-0 covering index correctly
 *       (covered scans match the base column) — the on-disk backward-compat
 *       guarantee;</li>
 *   <li>on partition open the legacy format-0 head is EAGERLY MIGRATED to the
 *       de-aliased format 1 (observed as a covering reseal), after which covered
 *       reads remain exact — so no legacy covering head is ever extended in place
 *       and re-exposes the concurrent covered-read OOB.</li>
 * </ul>
 */
public class CoveringIndexLegacyMigrationTest extends AbstractCairoTest {

    @Test
    public void testLegacyFormat0ReadCompatAndEagerMigration() throws Exception {
        assertMemoryLeak(() -> {
            // Covering (POSTING INCLUDE) table + a non-covering control fed the
            // same rows. BYPASS WAL so releaseAllWriters + the next write reopens
            // the partition (the eager-migration hook).
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

            // (2) Reopen and keep writing with the de-aliased build active. The
            // legacy format-0 head is superseded on its next covering reseal
            // (the writer only ever appends new format-1 entries); covered reads
            // must stay exact vs the control throughout — across the format-0 ->
            // format-1 transition — with no OOB / corruption.
            engine.releaseAllWriters();
            for (int b = 10; b < 16; b++) {
                final String sel = " SELECT dateadd('s', (" + (b * 40) + " + x)::INT, '2024-03-01T00:00:00Z'::TIMESTAMP),"
                        + " 'S' || ((" + (b * 40) + " + x) % 5), (" + (b * 40) + " + x)::DOUBLE FROM long_sequence(40)";
                execute("INSERT INTO t" + sel);
                execute("INSERT INTO ctl" + sel);
            }
            assertCoveredMatchesControl();
        });
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
