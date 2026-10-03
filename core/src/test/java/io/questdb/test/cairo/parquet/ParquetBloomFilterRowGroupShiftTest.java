/*+*****************************************************************************
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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * An in-place parquet O3 update that shifts row-group positions must keep each
 * row group's {@code _pm} bloom filter attached to that row group. The bloom
 * bitsets captured by the write are in write order, while the {@code _pm} footer
 * is in final order; when they were joined by position, a shifted row group got
 * another group's bloom filter and {@code WHERE val = v} skipped the row group
 * that holds {@code v}, returning no rows.
 * <p>
 * Every test builds a 16-row parquet partition (4 row groups of 4 rows, bloom
 * filter on {@code val}), applies one O3 batch or replace commit in place, then
 * checks the equality filter for every value in the partition. A replace commit
 * that covers a whole row group drops it in place, which shifts every later row
 * group down by one position.
 */
public class ParquetBloomFilterRowGroupShiftTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        // keep O3 in place: never escalate to a rewrite for dead bytes
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
    }

    @Test
    public void testDropFirstRowGroup() throws Exception {
        // DROP(rg0): rg1..rg3 all shift down one position.
        assertBloomAfterReplace("00:00", "03:30", rows());
    }

    @Test
    public void testDropMiddleRowGroupAndInsertInItsPlace() throws Exception {
        // DROP(rg1) plus one new row in the gap before it, which lands where rg1
        // was: rg2 and rg3 keep their positions but hold different groups' data.
        assertBloomAfterReplace("03:30", "07:59", rows(row(800, "03:30")));
    }

    @Test
    public void testDropTwoRowGroupsAndSplitMerge() throws Exception {
        // DROP(rg0) and DROP(rg2), MERGE into rg1 that splits into two groups:
        // [c0, c1, rg3], every group at a new position.
        assertBloomAfterReplace(
                "00:00", "11:59",
                rows(row(105, "04:00"), row(900, "04:10"), row(901, "04:20"), row(902, "04:30"), row(106, "05:00"), row(107, "06:00"), row(108, "07:00"))
        );
    }

    @Test
    public void testOneGapTwoNewGroups() throws Exception {
        // One 8-row gap between rg0 and rg1 becomes two new row groups:
        // [rg0, n1, n2, rg1, rg2, rg3].
        ObjList<String> rows = new ObjList<>();
        for (int i = 0; i < 8; i++) {
            rows.add(row(700 + i, String.format("03:%02d", 5 + 5 * i)));
        }
        assertBloomAfterO3(rows);
    }

    @Test
    public void testSingleMidFileInsert() throws Exception {
        // Control: one new group between rg0 and rg1.
        assertBloomAfterO3(rows(row(800, "03:30")));
    }

    @Test
    public void testSplitMerge() throws Exception {
        // MERGE into rg1 splits into two output groups: chunk0 replaces rg1 and
        // chunk1 is inserted after it.
        assertBloomAfterO3(rows(row(900, "05:10"), row(901, "05:20"), row(902, "05:30")));
    }

    @Test
    public void testSplitRg0Alone() throws Exception {
        // Control: rg0 splits into two groups at positions 0 and 1.
        assertBloomAfterO3(rows(row(900, "01:10"), row(901, "01:20"), row(902, "01:30")));
    }

    @Test
    public void testSplitRg0PlusLateInsert() throws Exception {
        // rg0 splits into two groups and a new group lands between rg2 and rg3:
        // the untouched rg1 and rg2 shift by one position.
        assertBloomAfterO3(rows(row(900, "01:10"), row(901, "01:20"), row(902, "01:30"), row(800, "11:30")));
    }

    @Test
    public void testTwoMidFileInserts() throws Exception {
        // Two new groups in one commit: [rg0, n1, rg1, n2, rg2, rg3].
        assertBloomAfterO3(rows(row(800, "03:30"), row(801, "07:30")));
    }

    private static long partitionNameTxn() {
        try (TableReader reader = engine.getReader("x")) {
            return reader.getTxFile().getPartitionNameTxn(0);
        }
    }

    private static String row(int val, String hhmm) {
        return val + "\t2024-01-01T" + hhmm + ":00.000000Z";
    }

    private static ObjList<String> rows(String... rows) {
        ObjList<String> list = new ObjList<>();
        for (String r : rows) {
            list.add(r);
        }
        return list;
    }

    private void assertBloomAfterO3(ObjList<String> o3Rows) throws Exception {
        assertMemoryLeak(() -> {
            createBloomPartition();

            ObjList<String> allRows = new ObjList<>();
            for (int i = 0; i < 16; i++) {
                allRows.add(row(101 + i, String.format("%02d:00", i)));
            }
            assertEveryValue(allRows);

            long nameTxnBefore = partitionNameTxn();
            StringBuilder insert = new StringBuilder("INSERT INTO x VALUES ");
            for (int i = 0, n = o3Rows.size(); i < n; i++) {
                String[] parts = o3Rows.getQuick(i).split("\t");
                insert.append(i > 0 ? ", " : "").append('(').append(parts[0]).append(", '").append(parts[1]).append("')");
            }
            execute(insert);
            drainWalQueue();
            // the O3 commit must have updated the parquet file in place
            Assert.assertEquals(nameTxnBefore, partitionNameTxn());
            assertQuery("SELECT count() FROM x WHERE ts IN '2024-01-01'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n" + (16 + o3Rows.size()) + "\n");

            allRows.addAll(o3Rows);
            assertEveryValue(allRows);
        });
    }

    /**
     * Commits a replace range [lo, hi] (hh:mm on 2024-01-01, inclusive to the
     * minute's end) that brings {@code newRows}, then checks that every surviving
     * old value and every new value is found through the bloom-filtered equality
     * filter and that every replaced old value is gone.
     */
    private void assertBloomAfterReplace(String lo, String hi, ObjList<String> newRows) throws Exception {
        assertMemoryLeak(() -> {
            createBloomPartition();
            final long rangeLo = MicrosTimestampDriver.floor("2024-01-01T" + lo + ":00.000000Z");
            final long rangeHi = MicrosTimestampDriver.floor("2024-01-01T" + hi + ":59.999999Z");
            final long nameTxnBefore = partitionNameTxn();
            try (WalWriter ww = engine.getWalWriter(engine.verifyTableName("x"))) {
                for (int i = 0, n = newRows.size(); i < n; i++) {
                    final String[] parts = newRows.getQuick(i).split("\t");
                    final TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor(parts[1]));
                    row.putLong(0, Long.parseLong(parts[0]));
                    row.append();
                }
                ww.commitWithParams(rangeLo, rangeHi + 1, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            // the replace commit must have updated the parquet file in place
            Assert.assertEquals(nameTxnBefore, partitionNameTxn());

            final ObjList<String> expected = new ObjList<>();
            for (int i = 0; i < 16; i++) {
                final long ts = MicrosTimestampDriver.floor(String.format("2024-01-01T%02d:00:00.000000Z", i));
                final String r = row(101 + i, String.format("%02d:00", i));
                if (ts < rangeLo || ts > rangeHi) {
                    expected.add(r);
                } else {
                    // replaced: gone unless the commit brought the same value back
                    boolean readded = false;
                    for (int j = 0, n = newRows.size(); j < n; j++) {
                        readded |= newRows.getQuick(j).equals(r);
                    }
                    if (!readded) {
                        assertQuery("SELECT val, ts FROM x WHERE val = " + (101 + i))
                                .noLeakCheck()
                                .timestamp("ts")
                                .returns("val\tts\n");
                    }
                }
            }
            expected.addAll(newRows);
            assertQuery("SELECT count() FROM x WHERE ts IN '2024-01-01'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n" + expected.size() + "\n");
            assertEveryValue(expected);
        });
    }

    private void assertEveryValue(ObjList<String> rows) throws Exception {
        for (int i = 0, n = rows.size(); i < n; i++) {
            String row = rows.getQuick(i);
            String val = row.substring(0, row.indexOf('\t'));
            assertQuery("SELECT val, ts FROM x WHERE val = " + val)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("val\tts\n" + row + "\n");
        }
    }

    private void createBloomPartition() throws Exception {
        execute("CREATE TABLE x (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        // 16 rows, hourly 00:00..15:00, val 101..116 -> 4 row groups of 4
        execute("INSERT INTO x SELECT (100 + x)::LONG, timestamp_sequence('2024-01-01', 3_600_000_000) FROM long_sequence(16)");
        // a later partition, so 2024-01-01 is not the active one and can convert
        execute("INSERT INTO x VALUES (1, '2024-01-02T00:00:00.000000Z')");
        drainWalQueue();
        execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01' WITH (bloom_filter_columns = 'val')");
        drainWalQueue();
    }
}
