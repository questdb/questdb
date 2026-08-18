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

package io.questdb.test.cairo.o3;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * TableWriter.changeColumnType's "open new column files" block positions the CONVERTED column's append
 * memory at {@code txWriter.getTransientRowCount()} - the partition's LIVE row count - instead of lifting
 * it to {@code E} (the physical extent) via {@code getLastPartitionFileRowCount(...)}, the way every other
 * analogous site does (section 14 of COMPOSITE_PARTITION_STATE.md), and the way the sibling method
 * {@code changeSymbolCapacity} does it by skipping the reopen entirely behind
 * {@code !isLastPartitionAppendBlocked()}.
 * <p>
 * The conversion itself (ConvertOperatorImpl, ahead of this block) already wrote the destination file out
 * to the partition's full physical extent - see section 17, "A column conversion walked the LIVE row
 * count, not E - FIXED". So the mapped {@code MemoryMA} this block opens is left positioned BELOW where
 * the file's real data ends. Nothing corrupts the file yet - {@code jumpTo} only moves an append cursor -
 * but the next thing that closes this mapping with {@code truncate=true} (a real writer close, or
 * {@code freeColumnMemory} on a later rename/convert/remove of this same column) truncates the file right
 * back down to that stale, too-low position, discarding whatever of the composite partition's tail sat
 * above it.
 * <p>
 * This test forces that close with {@code engine.releaseAllWriters()} right after the first conversion,
 * then converts the SAME column a second time. The second conversion's own source read goes through
 * {@code ColumnTypeConverter.convertFixedToFixed}, which throws "composite conversion source file too
 * short" ahead of the native call when the file undershoots what the partition's geometry says it should
 * hold - exactly the diagnostic that caught this defect's signature (a) in the wild. That exception
 * surfaces as the table going SUSPENDED once WAL apply processes the second ALTER.
 */
public class ScratchConvertColumnTypeUsesLiveRowCountTest extends AbstractCairoTest {

    @Test
    public void testColumnConvertedOnCompositePartitionSurvivesWriterClose() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "8K");

            // Day covers 00:00..20:00, one piece, 4800 live rows.
            final String base = "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                    " FROM long_sequence(4800)";
            execute("CREATE TABLE x AS (" + base + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // Backdated batch lands INSIDE the day: the merge-append rewrites (piece + batch) at the TAIL,
            // above the original 4800 (now dead) rows. E becomes 4800 + 5000 = 9800, live stays 5000 - and
            // the live piece occupies FILE ROWS [4800, 9800), not [0, 5000). A close that positions this
            // column's memory at the live COUNT (5000) rather than E truncates the file mid-piece, not just
            // through dead space.
            final String backfill = "SELECT x::INT + 70000 i, timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts" +
                    " FROM long_sequence(200)";
            execute("INSERT INTO x " + backfill);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            Assert.assertFalse("merge-append suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // First conversion on the now-composite last partition (live=5000, E=9800). ConvertOperatorImpl
            // writes the destination file out to E; changeColumnType's own reopen-and-position of that same
            // file is the defect under test.
            execute("ALTER TABLE x ALTER COLUMN i TYPE LONG");
            drainWalQueue();
            Assert.assertFalse("first column-type conversion suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // Force a REAL writer close (not a pool checkin) before anything else could reposition the
            // mis-set append offset. If the defect is present, this truncates the LONG file down to 5000
            // rows - chopping straight through the live piece, which starts at file row 4800.
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            // Second conversion of the SAME column reads the (possibly now-undersized) LONG file as its
            // source. If it was truncated, ColumnTypeConverter.convertFixedToFixed's own guard throws
            // "composite conversion source file too short" before the native call runs, and WAL apply
            // suspends the table on it.
            execute("ALTER TABLE x ALTER COLUMN i TYPE INT");
            drainWalQueue();
            Assert.assertFalse(
                    "second column-type conversion suspended the table - the first conversion's column " +
                            "memory was positioned at the live row count instead of the composite partition's " +
                            "physical extent, and closing the writer truncated real data away",
                    engine.getTableSequencerAPI().isSuspended(xt)
            );

            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize().returns("c\n5000\n");
            assertQuery("SELECT sum(i) s FROM x WHERE i BETWEEN 70001 AND 70200")
                    .noRandomAccess()
                    .expectSize()
                    .returns("s\n14020100\n");

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize().returns("c\n5000\n");
        });
    }
}
