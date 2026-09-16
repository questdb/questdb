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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * {@code ContiguousFileVarFrameColumn.merge} has to size the destination data vector BEFORE it maps it, and a
 * dedup merge index makes the two source slices added together the wrong answer: the index emits one entry per
 * data row, and a data row that collided with an incoming row carries the INCOMING row's id - so one incoming
 * value is written once per pre-existing duplicate key, and {@code (N-1)} extra copies of it have to fit in a
 * mapping that reserved none. Both merge-index producers behave this way:
 * {@code merge_dedup_long_index_int_keys} (dedup keys beside the timestamp) and
 * {@code mergeDedupTimestampWithLongIndexAsc} (timestamp-only dedup).
 * <p>
 * The pre-existing duplicates that make this reachable come from a table that ran WITHOUT dedup: enabling it
 * with {@code ALTER TABLE ... DEDUP ENABLE UPSERT KEYS(...)} does not rewrite the rows already on disk.
 */
public class CompositeDedupVarColumnMergeTest extends AbstractCairoTest {
    /**
     * Long enough that {@code (N-1)} extra copies overrun the page the under-sized mapping rounds up to,
     * which turns the mis-size into a SIGSEGV rather than a silent write into page slack.
     */
    private static final int LONG_VALUE_LEN = 100_000;
    private static final int LONG_ARRAY_LEN = 20_000;
    /**
     * The last characters of {@link #longString()}. A projection that slices them out reads the very END of the
     * value out of the DATA vector, which is what an under-sized mapping truncates - reading the length out of
     * the aux header alone would not notice.
     */
    private static final String LONG_VALUE_TAIL = "TAIL!";
    private static final String TAIL_QUERY = "SELECT ts, k, length(v) len, substring(v, "
            + (LONG_VALUE_LEN - LONG_VALUE_TAIL.length() + 1) + ", " + LONG_VALUE_TAIL.length() + ") tail FROM x";

    @Test
    public void testMergeAppendOffDedupRepeatsIncomingString() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            createTableWithThreeDuplicateKeys("STRING");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 1, '" + longString() + "')");
            drainWalQueue();
            assertQuery("SELECT ts, k, length(v) len FROM x").timestamp("ts").expectSize().returns(expectedLengths());
        });
    }

    @Test
    public void testMergeAppendOnDedupOnTimestampOnlyRepeatsIncomingString() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, k INT, v STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Three rows share a timestamp, which only a table without DEDUP can accumulate.
            execute("""
                    INSERT INTO x VALUES
                      ('2024-01-01T00:00:00.000000Z', 1, 'a'),
                      ('2024-01-01T00:00:00.000000Z', 2, 'bb'),
                      ('2024-01-01T00:00:00.000000Z', 3, 'ccc'),
                      ('2024-01-01T00:00:01.000000Z', 4, 'dddd')""");
            drainWalQueue();
            // Timestamp-only keys, so getDedupRows takes the Vect.mergeDedupTimestampWithLongIndexAsc branch.
            execute("ALTER TABLE x DEDUP ENABLE UPSERT KEYS(ts)");
            drainWalQueue();

            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 9, '" + longString() + "')");
            drainWalQueue();

            assertQuery("SELECT ts, k, length(v) len FROM x").timestamp("ts").expectSize().returns(
                    "ts\tk\tlen\n" +
                            "2024-01-01T00:00:00.000000Z\t9\t" + LONG_VALUE_LEN + "\n" +
                            "2024-01-01T00:00:00.000000Z\t9\t" + LONG_VALUE_LEN + "\n" +
                            "2024-01-01T00:00:00.000000Z\t9\t" + LONG_VALUE_LEN + "\n" +
                            "2024-01-01T00:00:01.000000Z\t4\t4\n"
            );
        });
    }

    @Test
    public void testMergeAppendOnDedupRepeatsIncomingArray() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, k INT, v DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO x VALUES
                      ('2024-01-01T00:00:00.000000Z', 1, ARRAY[1.0]),
                      ('2024-01-01T00:00:00.000000Z', 1, ARRAY[1.0, 2.0]),
                      ('2024-01-01T00:00:00.000000Z', 1, ARRAY[1.0, 2.0, 3.0]),
                      ('2024-01-01T00:00:01.000000Z', 2, ARRAY[1.0, 2.0, 3.0, 4.0])""");
            drainWalQueue();
            execute("ALTER TABLE x DEDUP ENABLE UPSERT KEYS(ts, k)");
            drainWalQueue();

            execute("INSERT INTO x SELECT '2024-01-01T00:00:00.000000Z'::TIMESTAMP, 1," +
                    " rnd_double_array(1, 0, 0, " + LONG_ARRAY_LEN + ") FROM long_sequence(1)");
            drainWalQueue();

            assertQuery("SELECT ts, k, dim_length(v, 1) len FROM x").timestamp("ts").expectSize().returns(
                    "ts\tk\tlen\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_ARRAY_LEN + "\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_ARRAY_LEN + "\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_ARRAY_LEN + "\n" +
                            "2024-01-01T00:00:01.000000Z\t2\t4\n"
            );
        });
    }

    @Test
    public void testMergeAppendOnDedupRepeatsIncomingBinary() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, k INT, v BINARY) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x SELECT '2024-01-01T00:00:00.000000Z'::TIMESTAMP, 1, rnd_bin(4, 4, 0)" +
                    " FROM long_sequence(3)");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:01.000000Z', 2, rnd_bin(4, 4, 0))");
            drainWalQueue();
            execute("ALTER TABLE x DEDUP ENABLE UPSERT KEYS(ts, k)");
            drainWalQueue();

            execute("INSERT INTO x SELECT '2024-01-01T00:00:00.000000Z'::TIMESTAMP, 1," +
                    " rnd_bin(" + LONG_VALUE_LEN + ", " + LONG_VALUE_LEN + ", 0) FROM long_sequence(1)");
            drainWalQueue();

            assertQuery("SELECT ts, k, length(v) len FROM x").timestamp("ts").expectSize().returns(
                    "ts\tk\tlen\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_VALUE_LEN + "\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_VALUE_LEN + "\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_VALUE_LEN + "\n" +
                            "2024-01-01T00:00:01.000000Z\t2\t4\n"
            );
        });
    }

    @Test
    public void testMergeAppendOnDedupRepeatsIncomingString() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        assertMemoryLeak(() -> {
            createTableWithThreeDuplicateKeys("STRING");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 1, '" + longString() + "')");
            drainWalQueue();
            assertQuery("SELECT ts, k, length(v) len FROM x").timestamp("ts").expectSize().returns(expectedLengths());
        });
    }

    @Test
    public void testMergeAppendOnDedupRepeatsIncomingStringBelowColumnTop() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        assertMemoryLeak(() -> {
            createTableWithColumnTop("STRING", "'e'");

            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 1, '" + longString() + "')");
            drainWalQueue();

            assertQuery("SELECT ts, k, length(v) len FROM x").timestamp("ts").expectSize().returns(
                    "ts\tk\tlen\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_VALUE_LEN + "\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_VALUE_LEN + "\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_VALUE_LEN + "\n" +
                            // length() of a NULL string is -1: this row sits BELOW v's column top, so the
                            // top-aware kernel wrote the type's NULL for it rather than reading an aux entry.
                            "2024-01-01T00:00:01.000000Z\t2\t-1\n" +
                            "2024-01-01T00:00:02.000000Z\t3\t1\n"
            );
        });
    }

    @Test
    public void testMergeAppendOnDedupRepeatsIncomingVarchar() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        assertMemoryLeak(() -> {
            createTableWithThreeDuplicateKeys("VARCHAR");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 1, '" + longString() + "')");
            drainWalQueue();
            assertQuery(TAIL_QUERY).timestamp("ts").expectSize().returns(
                    "ts\tk\tlen\ttail\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_VALUE_LEN + "\t" + LONG_VALUE_TAIL + "\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_VALUE_LEN + "\t" + LONG_VALUE_TAIL + "\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_VALUE_LEN + "\t" + LONG_VALUE_TAIL + "\n" +
                            // 'dddd' is shorter than the slice's start, so the slice is empty rather than NULL.
                            "2024-01-01T00:00:01.000000Z\t2\t4\t\n"
            );
        });
    }

    /**
     * The VARCHAR twin of {@link #testMergeAppendOnDedupRepeatsIncomingStringBelowColumnTop()}. VARCHAR is the
     * driver that makes the below-top sizing interesting: its {@code getDataVectorMinEntrySize()} is 0, not
     * STRING's 4, so a below-top row costs nothing at all and any row the walk charges wrongly changes the
     * mapping's size by a whole value.
     */
    @Test
    public void testMergeAppendOnDedupRepeatsIncomingVarcharBelowColumnTop() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        assertMemoryLeak(() -> {
            createTableWithColumnTop("VARCHAR", "'e'");

            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 1, '" + longString() + "')");
            drainWalQueue();

            assertQuery(TAIL_QUERY).timestamp("ts").expectSize().returns(
                    "ts\tk\tlen\ttail\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_VALUE_LEN + "\t" + LONG_VALUE_TAIL + "\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_VALUE_LEN + "\t" + LONG_VALUE_TAIL + "\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_VALUE_LEN + "\t" + LONG_VALUE_TAIL + "\n" +
                            // length() of a NULL varchar is -1: this row sits BELOW v's column top, so the
                            // top-aware kernel wrote the type's NULL for it rather than reading an aux entry.
                            "2024-01-01T00:00:01.000000Z\t2\t-1\t\n" +
                            "2024-01-01T00:00:02.000000Z\t3\t1\t\n"
            );
        });
    }

    /**
     * The ARRAY twin of {@link #testMergeAppendOnDedupRepeatsIncomingStringBelowColumnTop()}. Like VARCHAR,
     * ARRAY charges 0 bytes for a below-top NULL, and {@code dim_length()} reads the shape header out of the
     * data vector, so a truncated mapping cannot pass this assertion.
     */
    @Test
    public void testMergeAppendOnDedupRepeatsIncomingArrayBelowColumnTop() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        assertMemoryLeak(() -> {
            createTableWithColumnTop("DOUBLE[]", "ARRAY[1.0]");

            execute("INSERT INTO x SELECT '2024-01-01T00:00:00.000000Z'::TIMESTAMP, 1," +
                    " rnd_double_array(1, 0, 0, " + LONG_ARRAY_LEN + ") FROM long_sequence(1)");
            drainWalQueue();

            assertQuery("SELECT ts, k, dim_length(v, 1) len FROM x").timestamp("ts").expectSize().returns(
                    "ts\tk\tlen\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_ARRAY_LEN + "\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_ARRAY_LEN + "\n" +
                            "2024-01-01T00:00:00.000000Z\t1\t" + LONG_ARRAY_LEN + "\n" +
                            // A NULL array below v's column top has no dimension to measure.
                            "2024-01-01T00:00:01.000000Z\t2\tnull\n" +
                            "2024-01-01T00:00:02.000000Z\t3\t1\n"
            );
        });
    }

    private static String longString() {
        return "X".repeat(LONG_VALUE_LEN - LONG_VALUE_TAIL.length()) + LONG_VALUE_TAIL;
    }

    /**
     * Three rows that share (ts, k) and one that does not, all written BEFORE column {@code v} exists, so the
     * merge that follows reads rows 0..3 from below {@code v}'s column top and takes the top-aware kernel - the
     * shape the merge index's absolute row ids cannot address in the aux vector.
     */
    private void createTableWithColumnTop(String varType, String aboveTopValue) throws Exception {
        execute("CREATE TABLE x (ts TIMESTAMP, k INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO x VALUES
                  ('2024-01-01T00:00:00.000000Z', 1),
                  ('2024-01-01T00:00:00.000000Z', 1),
                  ('2024-01-01T00:00:00.000000Z', 1),
                  ('2024-01-01T00:00:01.000000Z', 2)""");
        drainWalQueue();
        // v starts at row 4.
        execute("ALTER TABLE x ADD COLUMN v " + varType);
        execute("INSERT INTO x VALUES ('2024-01-01T00:00:02.000000Z', 3, " + aboveTopValue + ")");
        drainWalQueue();
        execute("ALTER TABLE x DEDUP ENABLE UPSERT KEYS(ts, k)");
        drainWalQueue();
    }

    private void createTableWithThreeDuplicateKeys(String varType) throws Exception {
        execute("CREATE TABLE x (ts TIMESTAMP, k INT, v " + varType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        // Three rows share (ts, k), which only a table without DEDUP can accumulate.
        execute("""
                INSERT INTO x VALUES
                  ('2024-01-01T00:00:00.000000Z', 1, 'a'),
                  ('2024-01-01T00:00:00.000000Z', 1, 'bb'),
                  ('2024-01-01T00:00:00.000000Z', 1, 'ccc'),
                  ('2024-01-01T00:00:01.000000Z', 2, 'dddd')""");
        drainWalQueue();
        // ALTER does not rewrite the three rows already on disk, so they stay duplicates.
        execute("ALTER TABLE x DEDUP ENABLE UPSERT KEYS(ts, k)");
        drainWalQueue();
    }

    private String expectedLengths() {
        return "ts\tk\tlen\n" +
                "2024-01-01T00:00:00.000000Z\t1\t" + LONG_VALUE_LEN + "\n" +
                "2024-01-01T00:00:00.000000Z\t1\t" + LONG_VALUE_LEN + "\n" +
                "2024-01-01T00:00:00.000000Z\t1\t" + LONG_VALUE_LEN + "\n" +
                "2024-01-01T00:00:01.000000Z\t2\t4\n";
    }
}
