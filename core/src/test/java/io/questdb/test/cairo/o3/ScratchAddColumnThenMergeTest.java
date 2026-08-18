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

public class ScratchAddColumnThenMergeTest extends AbstractCairoTest {

    @Test
    public void testAddColumnOnCompositeActivePartitionThenMergeAndNewPiece() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "8K");

            // Day covers 00:00..20:00, leaving room above it for a NEW_PIECE batch inside the same day.
            final String base = "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                    " FROM long_sequence(4800)";
            execute("CREATE TABLE x AS (" + base + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // First backdated batch: while 2020-02-03 is still the ACTIVE partition, this cuts it and
            // merge-appends - the partition goes composite while still being the last one.
            final String backfill1 = "SELECT x::INT + 70000 i, timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts" +
                    " FROM long_sequence(200)";
            execute("INSERT INTO x " + backfill1);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            Assert.assertFalse("first merge-append suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // ADD COLUMN while the LAST partition is ALREADY composite: its top is recorded at E (physical
            // rows), not at the live row count.
            execute("ALTER TABLE x ADD COLUMN new_col STRING");
            drainWalQueue();
            Assert.assertFalse("add column suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // Second commit, ONE INSERT, TWO batches that land as DIFFERENT actions in the SAME
            // composite plan: backfill2 lands inside an existing (pre-new_col) piece and merges it;
            // newpiece sorts above everything the partition holds and founds a piece of its own.
            final String backfill2 = "SELECT x::INT + 80000 i, timestamp_sequence('2020-02-03T08:00:07', 5*1000000L) ts," +
                    " ('s2-' || x)::STRING new_col FROM long_sequence(100)";
            final String newpiece = "SELECT x::INT + 90000 i, timestamp_sequence('2020-02-03T21:00:00', 15*1000000L) ts," +
                    " ('s3-' || x)::STRING new_col FROM long_sequence(100)";
            execute("INSERT INTO x (i, ts, new_col) " + backfill2 + " UNION ALL " + newpiece);
            drainWalQueue();
            Assert.assertFalse("second merge-append suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // Force a REAL writer close (not a pool checkin) right here, before any conversion: new_col's
            // data was written entirely by the composite frame executor's own fds, and columns[]'s own
            // mapping for it never advanced past row 0. A close that trusts columns[]'s stale position
            // truncates new_col's .d file back to 0 bytes, and the query below then maps past the real
            // file - this is the crash this test exists to catch.
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertQuery("SELECT count(*) c FROM x WHERE new_col IS NOT NULL")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n200\n");

            // The conversion that crashed in the wild: STRING -> SYMBOL over a column added mid-stream on
            // an already-composite partition, later extended by a further merge-append.
            execute("ALTER TABLE x ALTER COLUMN new_col TYPE SYMBOL");
            drainWalQueue();
            Assert.assertFalse("column type conversion suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            assertQuery("SELECT count(*) c FROM x WHERE new_col IS NOT NULL")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n200\n");

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery("SELECT count(*) c FROM x WHERE new_col IS NOT NULL")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n200\n");
        });
    }

    @Test
    public void testFixedColumnAcrossManyMergeAppendCyclesThenConvert() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "8K");

            final String base = "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                    " FROM long_sequence(4800)";
            execute("CREATE TABLE x AS (" + base + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // Added while the partition is still ORDINARY - its top is the plain live row count.
            execute("ALTER TABLE x ADD COLUMN early_col LONG");
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");

            // Several merge-append cycles over the SAME still-active partition, each relocating a piece
            // that predates the column addition just as much as any other.
            final String backfill1 = "SELECT x::INT + 70000 i, timestamp_sequence('2020-02-03T02:00:07', 5*1000000L) ts" +
                    " FROM long_sequence(100)";
            execute("INSERT INTO x (i, ts) " + backfill1);
            drainWalQueue();
            Assert.assertFalse("cycle 1 suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            final String backfill2 = "SELECT x::INT + 80000 i, timestamp_sequence('2020-02-03T06:00:07', 5*1000000L) ts" +
                    " FROM long_sequence(100)";
            execute("INSERT INTO x (i, ts) " + backfill2);
            drainWalQueue();
            Assert.assertFalse("cycle 2 suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            final String backfill3 = "SELECT x::INT + 90000 i, timestamp_sequence('2020-02-03T10:00:07', 5*1000000L) ts" +
                    " FROM long_sequence(100)";
            final String newpiece = "SELECT x::INT + 100000 i, timestamp_sequence('2020-02-03T21:00:00', 15*1000000L) ts" +
                    " FROM long_sequence(100)";
            execute("INSERT INTO x (i, ts) " + backfill3 + " UNION ALL " + newpiece);
            drainWalQueue();
            Assert.assertFalse("cycle 3 suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            execute("ALTER TABLE x ALTER COLUMN early_col TYPE VARCHAR");
            drainWalQueue();
            Assert.assertFalse("early_col conversion suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            assertQuery("SELECT count(*) c FROM x").noRandomAccess().expectSize().returns("c\n5200\n");

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery("SELECT count(*) c FROM x").noRandomAccess().expectSize().returns("c\n5200\n");
        });
    }

    @Test
    public void testFixedColumnAddedOnCompositeActivePartitionThenMergeAndNewPiece() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "8K");

            // Day covers 00:00..20:00, leaving room above it for a NEW_PIECE batch inside the same day.
            final String base = "SELECT x::INT i, timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                    " FROM long_sequence(4800)";
            execute("CREATE TABLE x AS (" + base + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // First backdated batch: while 2020-02-03 is still the ACTIVE partition, this cuts it and
            // merge-appends - the partition goes composite while still being the last one.
            final String backfill1 = "SELECT x::INT + 70000 i, timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts" +
                    " FROM long_sequence(200)";
            execute("INSERT INTO x " + backfill1);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            Assert.assertFalse("first merge-append suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // ADD a FIXED-size column while the LAST partition is ALREADY composite: its top is recorded
            // at E (physical rows), not at the live row count - same shape as the STRING case above, but
            // exercising ContiguousFileFixFrameColumn instead of the var-size column path.
            execute("ALTER TABLE x ADD COLUMN new_col BYTE");
            drainWalQueue();
            Assert.assertFalse("add column suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // Second commit, ONE INSERT, TWO batches that land as DIFFERENT actions in the SAME
            // composite plan: backfill2 lands inside an existing (pre-new_col) piece and merges it;
            // newpiece sorts above everything the partition holds and founds a piece of its own.
            final String backfill2 = "SELECT x::INT + 80000 i, timestamp_sequence('2020-02-03T08:00:07', 5*1000000L) ts," +
                    " (x % 127)::BYTE new_col FROM long_sequence(100)";
            final String newpiece = "SELECT x::INT + 90000 i, timestamp_sequence('2020-02-03T21:00:00', 15*1000000L) ts," +
                    " (x % 127)::BYTE new_col FROM long_sequence(100)";
            execute("INSERT INTO x (i, ts, new_col) " + backfill2 + " UNION ALL " + newpiece);
            drainWalQueue();
            Assert.assertFalse("second merge-append suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            assertQuery("SELECT count(*) c FROM x WHERE new_col != 0")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n200\n");

            execute("ALTER TABLE x ALTER COLUMN new_col TYPE SHORT");
            drainWalQueue();
            Assert.assertFalse("column type conversion suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            assertQuery("SELECT count(*) c FROM x WHERE new_col != 0")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n200\n");

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery("SELECT count(*) c FROM x WHERE new_col != 0")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n200\n");
        });
    }

    @Test
    public void testRenamedAwayNameReusedByFixedColumnAddedOnComposite() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "8K");

            final String base = "SELECT x::INT i, rnd_symbol('AB','BC','CD') sym2," +
                    " timestamp_sequence('2020-02-03', 15*1000000L) ts FROM long_sequence(4800)";
            execute("CREATE TABLE x AS (" + base + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            final String backfill1 = "SELECT x::INT + 70000 i, rnd_symbol('AB','BC','CD') sym2," +
                    " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200)";
            execute("INSERT INTO x " + backfill1);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            Assert.assertFalse("first merge-append suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // Rename the original column away, then rename it again - mirroring the fuzz sequence that
            // exposed the bug (sym2 -> new_col_1 -> new_col_4) - freeing up the name "new_col_1".
            execute("ALTER TABLE x RENAME COLUMN sym2 TO new_col_1");
            execute("ALTER TABLE x RENAME COLUMN new_col_1 TO new_col_4");
            drainWalQueue();
            Assert.assertFalse("renames suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            // A BRAND NEW column reuses the freed name, added while the last partition is composite.
            execute("ALTER TABLE x ADD COLUMN new_col_1 BYTE");
            drainWalQueue();
            Assert.assertFalse("add column suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            final String backfill2 = "SELECT x::INT + 80000 i, rnd_symbol('AB','BC','CD') sym2," +
                    " timestamp_sequence('2020-02-03T08:00:07', 5*1000000L) ts, (x % 127)::BYTE new_col_1" +
                    " FROM long_sequence(100)";
            final String newpiece = "SELECT x::INT + 90000 i, rnd_symbol('AB','BC','CD') sym2," +
                    " timestamp_sequence('2020-02-03T21:00:00', 15*1000000L) ts, (x % 127)::BYTE new_col_1" +
                    " FROM long_sequence(100)";
            execute("INSERT INTO x (i, new_col_4, ts, new_col_1) " + backfill2 + " UNION ALL " + newpiece);
            drainWalQueue();
            Assert.assertFalse("second merge-append suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            assertQuery("SELECT count(*) c FROM x WHERE new_col_1 != 0")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n200\n");

            execute("ALTER TABLE x ALTER COLUMN new_col_1 TYPE SHORT");
            drainWalQueue();
            Assert.assertFalse("column type conversion suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            assertQuery("SELECT count(*) c FROM x WHERE new_col_1 != 0")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n200\n");

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery("SELECT count(*) c FROM x WHERE new_col_1 != 0")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n200\n");
        });
    }
}
