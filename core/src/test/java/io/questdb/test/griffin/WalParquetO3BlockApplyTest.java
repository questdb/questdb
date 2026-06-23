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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Regression for an O3-into-parquet apply that suspends the table.
 *
 * <p>When a WAL apply takes the optimised "all data in order, single segment"
 * block path ({@code TableWriter.processWalCommitBlock}), it maps the WAL
 * segment columns directly instead of copying them into a fresh 0-based buffer,
 * and sets {@code o3Lo = segmentCopyInfo.getRowLo(0)} -- the segment row offset
 * of the block. That offset is non-zero whenever the segment already carries
 * rows applied by an earlier block. For var-size columns the resulting O3 data
 * buffer is therefore a window {@code [dataOffset(rowLo), dataOffset(rowHi))}.
 *
 * <p>The parquet O3 path ({@code O3PartitionJob.copyO3ToRowGroup} ->
 * {@code populateO3DescriptorColumns}, the {@code COPY_O3} action) hands the
 * native parquet writer {@code primary_data = dataMem.addressOf(0)} together
 * with an <i>absolute</i> {@code dataExtent = getDataVectorSizeAt(aux, o3Hi)}.
 * The native encoder indexes {@code primary_data} with absolute aux offsets, so
 * it requires the data buffer to start at offset 0 -- which the windowed mapping
 * does not. When the inserted rows are null in a var column the window is empty,
 * {@code addressOf(0) == 0}, and the native {@code Column::from_raw_data} guard
 * "null primary_data_ptr with non-zero size" throws, which suspends the table.
 *
 * <p>This reproduces it with pure OSS SQL: convert a partition to parquet, bump
 * the WAL segment past row 0 with a native insert, then apply an in-order block
 * of O3 rows that land in a gap before the parquet partition's first row group
 * (a {@code COPY_O3} action) with the var column left null.
 *
 * <p>The sibling MERGE path is unaffected because it feeds the windowed buffers
 * to {@code Vect.*} merge functions that index pointer+offset directly and never
 * materialise the unmapped {@code [0, dataOffset(rowLo))} prefix.
 *
 * <p>The column-top case (a var column added after the partition was written) is
 * not covered here: it is awkward to set up against a converted parquet partition.
 */
public class WalParquetO3BlockApplyTest extends AbstractCairoTest {

    @Test
    public void testInOrderO3IntoParquetPartitionViaBlockApply() throws Exception {
        configureForCopyO3();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, s VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // Day 2024-01-01 holds the partition we convert. Its data starts at 06:00
            // so a later insert at 00:00 lands in a gap BEFORE the first row group
            // (a COPY_O3 action), not a merge into an existing row group. The VARCHAR
            // values are long (> 9 bytes) so they live in the data vector rather than
            // inlined in the aux -- the data vector must be non-empty for the bug to bite.
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-01T06:00:00.000000Z', 60_000_000L), 'value_long_string_' || x FROM long_sequence(200)");
            // A second partition so 2024-01-01 is not the active partition when converted.
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-02T00:00:00.000000Z', 60_000_000L), 'value_long_string_d2_' || x FROM long_sequence(5)");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            // A native insert AFTER the convert advances the WAL segment past row 0, so
            // the next apply block starts at a non-zero segment offset regardless of
            // whether the convert rolled the segment.
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-03T00:00:00.000000Z', 60_000_000L), 'value_long_string_d3_' || x FROM long_sequence(50)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("x");
            // Row-group count before the trigger; a COPY_O3 prepend bumps it by one.
            final long rowGroupsBefore = parquetRowGroupCount(tt, "2024-01-01");

            // The trigger: three ascending O3 inserts applied as ONE in-order,
            // single-segment block (no drain between them) into the 2024-01-01 parquet
            // partition, before its 06:00 minimum (COPY_O3), with the VARCHAR column
            // left null.
            execute("INSERT INTO x(ts) VALUES ('2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO x(ts) VALUES ('2024-01-01T00:00:01.000000Z')");
            execute("INSERT INTO x(ts) VALUES ('2024-01-01T00:00:02.000000Z')");
            drainWalQueue();

            Assert.assertFalse(
                    "WAL apply suspended the table: an in-order single-segment block apply of O3 rows into a "
                            + "parquet partition handed the parquet writer a windowed var-column buffer with an "
                            + "absolute data offset (O3PartitionJob.populateO3DescriptorColumns / COPY_O3)",
                    engine.getTableSequencerAPI().isSuspended(tt));

            // Guard against a future heuristic silently turning the trigger into a
            // (safe) MERGE: a COPY_O3 prepends a new row group, so the count must
            // rise by exactly one. A MERGE would fold the gap rows into the existing
            // first row group and leave the count unchanged, leaving the bug
            // unexercised while the rest of the test still passes.
            Assert.assertEquals(
                    "trigger did not take the COPY_O3 prepend path",
                    rowGroupsBefore + 1,
                    parquetRowGroupCount(tt, "2024-01-01")
            );

            // All rows must be present (200 + 5 + 50 + 3) and the parquet partition still readable.
            // count() over a table with a parquet partition has no random access and a known size.
            assertQuery("SELECT count() FROM x").noRandomAccess().expectSize().returns("count\n258\n");
        });
    }

    @Test
    public void testInOrderO3IntoParquetWithArrayColumn() throws Exception {
        // ARRAY uses ArrayTypeDriver -- a distinct var-size driver (not a
        // StringTypeDriver subclass) with its own shiftCopyAuxVector. Non-null
        // arrays in the trigger rows exercise the real-data rebase for it.
        configureForCopyO3();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-01T06:00:00.000000Z', 60_000_000L), ARRAY[x::double, (x * 2)::double] FROM long_sequence(200)");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-02T00:00:00.000000Z', 60_000_000L), ARRAY[x::double, (x * 2)::double] FROM long_sequence(5)");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-03T00:00:00.000000Z', 60_000_000L), ARRAY[x::double, (x * 2)::double] FROM long_sequence(50)");
            drainWalQueue();

            // Trigger: three ascending O3 inserts applied as one in-order block
            // into the parquet partition, before 06:00 (COPY_O3), with non-null
            // arrays.
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', ARRAY[1.5, 2.5, 3.5])");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:01.000000Z', ARRAY[10.5, 20.5, 30.5])");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:02.000000Z', ARRAY[100.5, 200.5, 300.5])");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("x");
            Assert.assertFalse("WAL apply suspended the table", engine.getTableSequencerAPI().isSuspended(tt));

            assertQuery("SELECT count() FROM x").noRandomAccess().expectSize().returns("count\n258\n");
            // The trigger arrays round-trip element-for-element from the parquet
            // partition.
            assertQuery("SELECT ts, arr[1] e1, arr[2] e2, arr[3] e3 FROM x WHERE ts < '2024-01-01T06:00:00.000000Z' ORDER BY ts")
                    .timestamp("ts")
                    .sizeMayVary()
                    .returns(
                            "ts\te1\te2\te3\n" +
                                    "2024-01-01T00:00:00.000000Z\t1.5\t2.5\t3.5\n" +
                                    "2024-01-01T00:00:01.000000Z\t10.5\t20.5\t30.5\n" +
                                    "2024-01-01T00:00:02.000000Z\t100.5\t200.5\t300.5\n"
                    );
        });
    }

    @Test
    public void testInOrderO3IntoParquetWithBinaryColumn() throws Exception {
        // BINARY shares StringTypeDriver, so it rebases via the same shift_copy
        // primitive as STRING. Non-null trigger binaries exercise the real-data
        // (non-empty-window) rebase: addressOf(dataLo) + window-relative aux.
        configureForCopyO3();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, b BINARY) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Existing partition data starts at 06:00 with non-null binaries, so
            // the binary data vector is non-empty before the trigger rows.
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-01T06:00:00.000000Z', 60_000_000L), rnd_bin(20, 40, 0) FROM long_sequence(200)");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-02T00:00:00.000000Z', 60_000_000L), rnd_bin(20, 40, 0) FROM long_sequence(5)");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            // Advance the WAL segment past row 0 so the trigger block starts at a
            // non-zero segment offset.
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-03T00:00:00.000000Z', 60_000_000L), rnd_bin(20, 40, 0) FROM long_sequence(50)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("x");
            final long rowGroupsBefore = parquetRowGroupCount(tt, "2024-01-01");

            // Three ascending O3 inserts applied as one in-order block into the
            // parquet partition, before 06:00 (COPY_O3), with KNOWN non-null
            // binaries (from_base64 yields deterministic bytes, unlike rnd_bin).
            // Distinct lengths (4/6/9) and contents so the round-trip below catches
            // a wrong data base/offset, not just a non-null one.
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', from_base64('3q2+7w=='))");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:01.000000Z', from_base64('AQIDBAUG'))");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:02.000000Z', from_base64('q83vECAwQFBg'))");
            drainWalQueue();

            Assert.assertFalse("WAL apply suspended the table", engine.getTableSequencerAPI().isSuspended(tt));
            // A COPY_O3 prepended a row group; a MERGE would have left the count
            // unchanged and not exercised the windowed-buffer rebase.
            Assert.assertEquals(
                    "trigger did not take the COPY_O3 prepend path",
                    rowGroupsBefore + 1,
                    parquetRowGroupCount(tt, "2024-01-01")
            );

            assertQuery("SELECT count() FROM x").noRandomAccess().expectSize().returns("count\n258\n");
            // The trigger binaries round-trip byte-for-byte from the parquet
            // partition (rendered as a hex dump) with their exact lengths; a wrong
            // rebase base/offset would corrupt the bytes, the length, or the apply.
            // Values are STORED then read back, so they are stable across the two
            // cursor passes returns() makes.
            assertQuery("SELECT ts, b, length(b) len FROM x WHERE ts < '2024-01-01T06:00:00.000000Z' ORDER BY ts")
                    .timestamp("ts")
                    .sizeMayVary()
                    .returns(
                            "ts\tb\tlen\n" +
                                    "2024-01-01T00:00:00.000000Z\t00000000 de ad be ef\t4\n" +
                                    "2024-01-01T00:00:01.000000Z\t00000000 01 02 03 04 05 06\t6\n" +
                                    "2024-01-01T00:00:02.000000Z\t00000000 ab cd ef 10 20 30 40 50 60\t9\n"
                    );
        });
    }

    @Test
    public void testInOrderO3IntoParquetWithMixedVarcharWindow() throws Exception {
        // A partial window mixing all three VARCHAR aux shapes in the trigger
        // block: one null, one fully inlined (<= 9 bytes, lives in the aux), and
        // one long (> 9 bytes, lives in the data vector). Only the long value
        // contributes bytes to the data window, so the rebase must keep its
        // offset correct while the null and inlined entries carry no data offset.
        configureForCopyO3();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-01T06:00:00.000000Z', 60_000_000L), 'varchar_existing_' || x FROM long_sequence(200)");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-02T00:00:00.000000Z', 60_000_000L), 'varchar_d2_' || x FROM long_sequence(5)");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-03T00:00:00.000000Z', 60_000_000L), 'varchar_d3_' || x FROM long_sequence(50)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("x");
            final long rowGroupsBefore = parquetRowGroupCount(tt, "2024-01-01");

            // Trigger: null, inlined (3 bytes), long (> 9 bytes), in ascending
            // order, applied as one in-order block before 06:00 (COPY_O3).
            execute("INSERT INTO x(ts) VALUES ('2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:01.000000Z', 'abc')");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:02.000000Z', 'varchar_inline_boundary_long')");
            drainWalQueue();

            Assert.assertFalse("WAL apply suspended the table", engine.getTableSequencerAPI().isSuspended(tt));
            Assert.assertEquals(
                    "trigger did not take the COPY_O3 prepend path",
                    rowGroupsBefore + 1,
                    parquetRowGroupCount(tt, "2024-01-01")
            );

            assertQuery("SELECT count() FROM x").noRandomAccess().expectSize().returns("count\n258\n");
            // The null stays null, the inlined and long values round-trip exactly;
            // a wrong rebase would corrupt the long value's bytes or its offset.
            assertQuery("SELECT ts, v FROM x WHERE ts < '2024-01-01T06:00:00.000000Z' ORDER BY ts")
                    .timestamp("ts")
                    .sizeMayVary()
                    .returns(
                            "ts\tv\n" +
                                    "2024-01-01T00:00:00.000000Z\t\n" +
                                    "2024-01-01T00:00:01.000000Z\tabc\n" +
                                    "2024-01-01T00:00:02.000000Z\tvarchar_inline_boundary_long\n"
                    );
        });
    }

    @Test
    public void testInOrderO3IntoParquetWithNonNullVarSizeColumns() throws Exception {
        // Two var columns (VARCHAR + STRING) with non-null long (> 9 byte) values
        // in the trigger rows. This exercises the real-data (non-empty-window)
        // rebase -- addressOf(dataLo) + window-relative aux -- for both the
        // shift_copy_varchar_aux (VARCHAR) and shift_copy (STRING) primitives,
        // with both rebased columns coexisting in the scratch arena.
        configureForCopyO3();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v VARCHAR, s STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-01T06:00:00.000000Z', 60_000_000L), 'varchar_existing_' || x, 'string_existing_' || x FROM long_sequence(200)");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-02T00:00:00.000000Z', 60_000_000L), 'varchar_d2_' || x, 'string_d2_' || x FROM long_sequence(5)");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-03T00:00:00.000000Z', 60_000_000L), 'varchar_d3_' || x, 'string_d3_' || x FROM long_sequence(50)");
            drainWalQueue();

            // Trigger: three ascending O3 inserts applied as one in-order block,
            // landing before 06:00 (COPY_O3), with non-null long values in both
            // var columns.
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 'varchar_trigger_alpha', 'string_trigger_alpha')");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:01.000000Z', 'varchar_trigger_bravo', 'string_trigger_bravo')");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:02.000000Z', 'varchar_trigger_charlie', 'string_trigger_charlie')");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("x");
            Assert.assertFalse("WAL apply suspended the table", engine.getTableSequencerAPI().isSuspended(tt));

            assertQuery("SELECT count() FROM x").noRandomAccess().expectSize().returns("count\n258\n");
            // The trigger rows' values round-trip exactly from the parquet
            // partition; a wrong rebase base/offset would corrupt or misalign them.
            assertQuery("SELECT ts, v, s FROM x WHERE ts < '2024-01-01T06:00:00.000000Z' ORDER BY ts")
                    .timestamp("ts")
                    .sizeMayVary()
                    .returns(
                            "ts\tv\ts\n" +
                                    "2024-01-01T00:00:00.000000Z\tvarchar_trigger_alpha\tstring_trigger_alpha\n" +
                                    "2024-01-01T00:00:01.000000Z\tvarchar_trigger_bravo\tstring_trigger_bravo\n" +
                                    "2024-01-01T00:00:02.000000Z\tvarchar_trigger_charlie\tstring_trigger_charlie\n"
                    );
        });
    }

    @Test
    public void testInOrderO3IntoParquetWithThreeVarSizeDrivers() throws Exception {
        // Three distinct var-size drivers in one table -- STRING, VARCHAR and
        // ARRAY -- all rebased in the same apply. STRING sizes its aux as
        // (n+1)*8 with a trailing sentinel; VARCHAR and ARRAY use 16*n. Packing
        // those heterogeneous slot strides back-to-back in the scratch arena
        // would expose a stride/sizing mistake in the bump allocator.
        configureForCopyO3();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, s STRING, v VARCHAR, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-01T06:00:00.000000Z', 60_000_000L), 'string_existing_' || x, 'varchar_existing_' || x, ARRAY[x::double, (x * 2)::double] FROM long_sequence(200)");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-02T00:00:00.000000Z', 60_000_000L), 'string_d2_' || x, 'varchar_d2_' || x, ARRAY[x::double, (x * 2)::double] FROM long_sequence(5)");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-03T00:00:00.000000Z', 60_000_000L), 'string_d3_' || x, 'varchar_d3_' || x, ARRAY[x::double, (x * 2)::double] FROM long_sequence(50)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("x");
            final long rowGroupsBefore = parquetRowGroupCount(tt, "2024-01-01");

            // Trigger: three ascending O3 rows before 06:00 (COPY_O3) with non-null
            // long values in all three var columns.
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 'string_trigger_alpha', 'varchar_trigger_alpha', ARRAY[1.5, 2.5])");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:01.000000Z', 'string_trigger_bravo', 'varchar_trigger_bravo', ARRAY[10.5, 20.5])");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:02.000000Z', 'string_trigger_charlie', 'varchar_trigger_charlie', ARRAY[100.5, 200.5])");
            drainWalQueue();

            Assert.assertFalse("WAL apply suspended the table", engine.getTableSequencerAPI().isSuspended(tt));
            Assert.assertEquals(
                    "trigger did not take the COPY_O3 prepend path",
                    rowGroupsBefore + 1,
                    parquetRowGroupCount(tt, "2024-01-01")
            );

            assertQuery("SELECT count() FROM x").noRandomAccess().expectSize().returns("count\n258\n");
            // All three drivers round-trip exactly; a stride mistake in the arena
            // would corrupt one column's bytes or its offsets.
            assertQuery("SELECT ts, s, v, arr[1] e1, arr[2] e2 FROM x WHERE ts < '2024-01-01T06:00:00.000000Z' ORDER BY ts")
                    .timestamp("ts")
                    .sizeMayVary()
                    .returns(
                            "ts\ts\tv\te1\te2\n" +
                                    "2024-01-01T00:00:00.000000Z\tstring_trigger_alpha\tvarchar_trigger_alpha\t1.5\t2.5\n" +
                                    "2024-01-01T00:00:01.000000Z\tstring_trigger_bravo\tvarchar_trigger_bravo\t10.5\t20.5\n" +
                                    "2024-01-01T00:00:02.000000Z\tstring_trigger_charlie\tvarchar_trigger_charlie\t100.5\t200.5\n"
                    );
        });
    }

    @Test
    public void testInOrderO3IntoTwoParquetPartitions() throws Exception {
        // One in-order block whose rows fan out into TWO different parquet
        // partitions, each before its own 06:00 minimum (a COPY_O3 per
        // partition). The apply rebases the same windowed source buffer once per
        // partition iteration, so a base/offset mistake would surface in at least
        // one of them.
        configureForCopyO3();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Both partitions start at 06:00 so a 00:00 insert lands in a gap.
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-01T06:00:00.000000Z', 60_000_000L), 'varchar_d1_' || x FROM long_sequence(200)");
            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-02T06:00:00.000000Z', 60_000_000L), 'varchar_d2_' || x FROM long_sequence(200)");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
            drainWalQueue();

            execute("INSERT INTO x SELECT timestamp_sequence('2024-01-03T00:00:00.000000Z', 60_000_000L), 'varchar_d3_' || x FROM long_sequence(50)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("x");
            final long rg1Before = parquetRowGroupCount(tt, "2024-01-01");
            final long rg2Before = parquetRowGroupCount(tt, "2024-01-02");

            // Trigger: two ascending O3 rows, one per parquet partition, each
            // before that partition's 06:00 minimum, applied as one in-order block.
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 'varchar_trigger_d1')");
            execute("INSERT INTO x VALUES ('2024-01-02T00:00:00.000000Z', 'varchar_trigger_d2')");
            drainWalQueue();

            Assert.assertFalse("WAL apply suspended the table", engine.getTableSequencerAPI().isSuspended(tt));
            // Each partition got a COPY_O3 prepend.
            Assert.assertEquals("2024-01-01 did not take the COPY_O3 prepend path", rg1Before + 1, parquetRowGroupCount(tt, "2024-01-01"));
            Assert.assertEquals("2024-01-02 did not take the COPY_O3 prepend path", rg2Before + 1, parquetRowGroupCount(tt, "2024-01-02"));

            assertQuery("SELECT count() FROM x").noRandomAccess().expectSize().returns("count\n452\n");
            // Each trigger value round-trips from its own parquet partition.
            assertQuery("SELECT ts, v FROM x WHERE v LIKE 'varchar_trigger_%' ORDER BY ts")
                    .timestamp("ts")
                    .sizeMayVary()
                    .returns(
                            "ts\tv\n" +
                                    "2024-01-01T00:00:00.000000Z\tvarchar_trigger_d1\n" +
                                    "2024-01-02T00:00:00.000000Z\tvarchar_trigger_d2\n"
                    );
        });
    }

    // Configures the engine so the trigger inserts land as a COPY_O3 action into
    // an existing parquet partition.
    private void configureForCopyO3() {
        // Keep everything in as few WAL segments as possible so the trigger block
        // starts at a non-zero segment row offset (getRowLo(0) > 0).
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10_000_000);
        // Small parquet row groups so the gap-before-first-row-group inserts stay a
        // COPY_O3 action: the merge strategy folds gap rows into an adjacent row
        // group only when that group is "small" (< rowGroupSize / 4), which a large
        // default would make the 200-row group, turning the trigger into a (safe)
        // MERGE instead.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
    }

    // Row-group count of the parquet partition that contains partitionName (e.g.
    // "2024-01-01"). A COPY_O3 prepends a new row group to the file, so the count
    // rises by one; a MERGE folds the gap rows into the existing first row group
    // and leaves it unchanged. Comparing before vs after the trigger therefore
    // proves the apply took the COPY_O3 prepend path the bug lives in. Goes
    // through the reader so it resolves the txn-suffixed partition dir itself.
    private long parquetRowGroupCount(TableToken tt, String partitionName) {
        final long partitionTs = parseFloorPartialTimestamp(partitionName);
        try (TableReader reader = engine.getReader(tt)) {
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getPartitionTimestampByIndex(i) != partitionTs) {
                    continue;
                }
                Assert.assertEquals(
                        "partition " + partitionName + " is not parquet",
                        PartitionFormat.PARQUET,
                        reader.getPartitionFormat(i)
                );
                reader.openPartition(i);
                return reader.getAndInitParquetPartitionDecoder(i).metadata().getRowGroupCount();
            }
        }
        throw new AssertionError("no partition for " + partitionName);
    }
}
