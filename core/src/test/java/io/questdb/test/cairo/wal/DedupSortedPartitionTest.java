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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The dedup sorted bit of a native partition in {@code _txn}: set while the rows of each timestamp are stored in
 * the order of the current dedup keys, and the physical order that goes with it. A dedup commit's rows arrive in
 * key order, so a new partition and an append have it, a merge puts the rows it writes in it, and a partition
 * written without it picks it up when a merge rewrites it. Every change of the dedup setting clears the bit.
 */
public class DedupSortedPartitionTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        // The classic O3 merge: a partition is one run of rows. Merge-append has its own tests.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
    }

    @Test
    public void testDedupSettingChangeClearsBit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, k LONG, s SYMBOL, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k, s)");
            final TableToken token = engine.verifyTableName("x");
            insertTwoDays();
            assertSortedBits(token, true, true);

            // The same keys again: the setting is still a new one.
            execute("ALTER TABLE x DEDUP ENABLE UPSERT KEYS(ts, k, s)");
            drainWalQueue();
            assertSortedBits(token, false, false);
            // The bit comes back with the partition that is written next, not with the setting.
            execute("INSERT INTO x VALUES ('2024-01-02T00:00:03', 1, 'a', 1)");
            drainWalQueue();
            assertSortedBits(token, false, false);
            execute("INSERT INTO x VALUES ('2024-01-03', 1, 'a', 1)");
            drainWalQueue();
            assertSortedBits(token, false, false, true);

            execute("ALTER TABLE x DEDUP DISABLE");
            drainWalQueue();
            assertSortedBits(token, false, false, false);
            // Without dedup no commit sets it.
            execute("INSERT INTO x VALUES ('2024-01-04', 1, 'a', 1)");
            drainWalQueue();
            assertSortedBits(token, false, false, false, false);

            execute("ALTER TABLE x DEDUP ENABLE UPSERT KEYS(ts, k, s)");
            execute("INSERT INTO x VALUES ('2024-01-05', 1, 'a', 1)");
            drainWalQueue();
            assertSortedBits(token, false, false, false, false, true);

            // A key column changes its type: its values compare in a new order.
            execute("ALTER TABLE x ALTER COLUMN s TYPE VARCHAR");
            drainWalQueue();
            assertSortedBits(token, false, false, false, false, false);
            execute("INSERT INTO x VALUES ('2024-01-06', 1, 'a', 1)");
            drainWalQueue();
            assertSortedBits(token, false, false, false, false, false, true);

            // The converted column now follows s in the table, while it precedes it by column id: WAL apply
            // sorts the keys in table order and the bit promises column id order, so no commit sets it.
            execute("ALTER TABLE x ALTER COLUMN k TYPE INT");
            drainWalQueue();
            assertSortedBits(token, false, false, false, false, false, false);
            execute("INSERT INTO x VALUES ('2024-01-07', 1, 'a', 1)");
            drainWalQueue();
            assertSortedBits(token, false, false, false, false, false, false, false);

            // A key column is dropped: one key is left and the bit can be set again.
            execute("ALTER TABLE x DROP COLUMN s");
            execute("INSERT INTO x VALUES ('2024-01-08', 1, 1)");
            drainWalQueue();
            assertSortedBits(token, false, false, false, false, false, false, false, true);
        });
    }

    @Test
    public void testMergeKeepsKeyOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, k LONG, s SYMBOL, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k, s)");
            final TableToken token = engine.verifyTableName("x");
            insertTwoDays();
            assertSortedBits(token, true, true);
            assertKeyOrder(token);

            // Into the first day: keys that sort before, between and after the stored ones of a timestamp, two
            // replacements, a timestamp between stored ones and a timestamp past the stored ones.
            execute("""
                    INSERT INTO x VALUES
                    ('2024-01-01T00:00:01', 100, 'b', 1000),
                    ('2024-01-01T00:00:01', -5, 'z', 1001),
                    ('2024-01-01T00:00:01', 3, 'a', 1002),
                    ('2024-01-01T00:00:01', 2, 'b', 1003),
                    ('2024-01-01T00:00:01', 2, 'a', 1004),
                    ('2024-01-01T00:00:01.5', 9, 'c', 1005),
                    ('2024-01-01T00:00:01.5', 1, 'c', 1006),
                    ('2024-01-01T12:00:00', 7, 'a', 1007),
                    ('2024-01-01T12:00:00', 4, 'a', 1008)
                    """);
            drainWalQueue();
            assertSortedBits(token, true, true);
            assertKeyOrder(token);
            assertQuery("SELECT * FROM x WHERE ts IN '2024-01-01' ORDER BY ts, k, s").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns("""
                    ts\tk\ts\tv
                    2024-01-01T00:00:01.000000Z\t-5\tz\t1001
                    2024-01-01T00:00:01.000000Z\t2\ta\t1004
                    2024-01-01T00:00:01.000000Z\t2\tb\t1003
                    2024-01-01T00:00:01.000000Z\t3\ta\t1002
                    2024-01-01T00:00:01.000000Z\t3\tb\t1
                    2024-01-01T00:00:01.000000Z\t100\tb\t1000
                    2024-01-01T00:00:01.500000Z\t1\tc\t1006
                    2024-01-01T00:00:01.500000Z\t9\tc\t1005
                    2024-01-01T00:00:02.000000Z\t1\tc\t5
                    2024-01-01T00:00:02.000000Z\t9\ta\t4
                    2024-01-01T12:00:00.000000Z\t4\ta\t1008
                    2024-01-01T12:00:00.000000Z\t7\ta\t1007
                    """);

            // A commit that starts at the partition's last timestamp with a smaller key is merged, not appended.
            execute("INSERT INTO x VALUES ('2024-01-02T00:00:02', -1, 'a', 2000), ('2024-01-02T00:00:09', 5, 'a', 2001), ('2024-01-02T00:00:09', 4, 'a', 2002)");
            drainWalQueue();
            assertSortedBits(token, true, true);
            assertKeyOrder(token);
            assertQuery("SELECT * FROM x WHERE ts IN '2024-01-02' ORDER BY ts, k, s").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns("""
                    ts\tk\ts\tv
                    2024-01-02T00:00:01.000000Z\t6\ta\t6
                    2024-01-02T00:00:02.000000Z\t-1\ta\t2000
                    2024-01-02T00:00:02.000000Z\t4\ta\t8
                    2024-01-02T00:00:02.000000Z\t5\tb\t7
                    2024-01-02T00:00:09.000000Z\t4\ta\t2002
                    2024-01-02T00:00:09.000000Z\t5\ta\t2001
                    """);

            // SKIP keeps the stored rows and still adds the new keys in order. The first day was written in key
            // order, which the merge finds out about the rows it copies.
            execute("ALTER TABLE x DEDUP ENABLE SKIP KEYS(ts, k, s)");
            drainWalQueue();
            assertSortedBits(token, false, false);
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:01', 2, 'a', 3000), ('2024-01-01T00:00:01', 0, 'a', 3001), ('2024-01-01T00:00:01', 200, 'a', 3002)");
            drainWalQueue();
            assertSortedBits(token, true, false);
            assertKeyOrder(token);
            assertQuery("SELECT * FROM x WHERE ts = '2024-01-01T00:00:01' ORDER BY ts, k, s").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns("""
                    ts\tk\ts\tv
                    2024-01-01T00:00:01.000000Z\t-5\tz\t1001
                    2024-01-01T00:00:01.000000Z\t0\ta\t3001
                    2024-01-01T00:00:01.000000Z\t2\ta\t1004
                    2024-01-01T00:00:01.000000Z\t2\tb\t1003
                    2024-01-01T00:00:01.000000Z\t3\ta\t1002
                    2024-01-01T00:00:01.000000Z\t3\tb\t1
                    2024-01-01T00:00:01.000000Z\t100\tb\t1000
                    2024-01-01T00:00:01.000000Z\t200\ta\t3002
                    """);
        });
    }

    @Test
    public void testNewPartitionAndAppendHaveKeyOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, k LONG, s SYMBOL, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k, s)");
            final TableToken token = engine.verifyTableName("x");
            insertTwoDays();
            assertSortedBits(token, true, true);
            assertKeyOrder(token);
            // An append to the last partition and a new partition before the first one.
            execute("INSERT INTO x VALUES ('2024-01-02T10:00', 3, 'a', 1), ('2024-01-02T10:00', 1, 'b', 2), ('2023-12-31', 5, 'a', 3), ('2023-12-31', 4, 'b', 4)");
            drainWalQueue();
            assertSortedBits(token, true, true, true);
            assertKeyOrder(token);
            // An UPDATE rewrites column files without regard to the key order.
            execute("UPDATE x SET v = 0 WHERE ts = '2023-12-31'");
            drainWalQueue();
            assertSortedBits(token, false, true, true);
        });
    }

    @Test
    public void testPartitionPicksUpKeyOrderOnRewrite() throws Exception {
        assertMemoryLeak(() -> {
            // Written without dedup: the rows of a timestamp are in insertion order.
            execute("CREATE TABLE x (ts TIMESTAMP, k LONG, s SYMBOL, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE oracle (ts TIMESTAMP, k LONG, s SYMBOL, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("x");
            // Day one: every timestamp holds four rows with descending keys. Day two: one row per timestamp.
            executeBoth("INSERT INTO %s SELECT dateadd('s', ((x - 1) / 4)::INT, '2024-01-01'::TIMESTAMP), 1000 - x, 's' || (x % 3), x::INT FROM long_sequence(400)");
            executeBoth("INSERT INTO %s SELECT dateadd('s', x::INT, '2024-01-02'::TIMESTAMP), 1000 - x, 's' || (x % 3), x::INT FROM long_sequence(100)");
            // The oracle stays without dedup: no row below repeats a key, so both tables hold the same rows.
            execute("ALTER TABLE x DEDUP ENABLE UPSERT KEYS(ts, k, s)");
            drainWalQueue();
            assertSortedBits(token, false, false);

            // Day two has no two rows with one timestamp: a merge into its middle finds the rows it copies in
            // order as they are.
            executeBoth("INSERT INTO %s VALUES ('2024-01-02T00:00:50', 7, 'a', -1), ('2024-01-02T00:00:50', 3, 'a', -2)");
            assertSortedBits(token, false, true);
            assertKeyOrder(token, 1);
            assertOracle();

            // Day one: a merge into one timestamp in the middle. The rows before and after it are out of key
            // order, so all of them go through the merge.
            executeBoth("INSERT INTO %s VALUES ('2024-01-01T00:00:50', 7, 'a', -3)");
            assertSortedBits(token, true, true);
            assertKeyOrder(token);
            assertOracle();
        });
    }

    @Test
    public void testPicksUpKeyOrderWithVarSizeKeysAndColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            createVarSizeKeyTable();
            final TableToken token = engine.verifyTableName("x");
            assertSortedBits(token, false, false);
            // A new key into a timestamp below the column tops: the merge takes every row of the day, whose
            // timestamps are out of key order, and writes them in order.
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:20', 'zz', 's1', -1, 5, 'q')");
            drainWalQueue();
            assertSortedBits(token, true, false);
            assertVarSizeKeyOrder();
            // Keys of every kind into timestamps on both sides of the column tops: the bit stays.
            execute("""
                    INSERT INTO x VALUES
                    ('2024-01-01T00:00:20', NULL, NULL, -2, NULL, NULL),
                    ('2024-01-01T00:00:20', 'a', 's0', -3, 1, 'a-varchar-longer-than-the-inlined-size'),
                    ('2024-01-01T00:00:20', 'a', 's0', -4, 1, 'b'),
                    ('2024-01-01T00:00:52', 'w0', 'a', -5, 0, 'kk'),
                    ('2024-01-01T00:00:52', 'a-varchar-longer-than-the-inlined-size', 'bb', -6, -7, NULL),
                    ('2024-01-01T00:00:52', 'w0', 'a', -7, NULL, 'kk')
                    """);
            drainWalQueue();
            assertSortedBits(token, true, false);
            assertVarSizeKeyOrder();
            assertQuery("SELECT count() c, sum(v) v FROM x WHERE ts IN '2024-01-01'").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary()
                    .returns("c\tv\n247\t" + (200L * 201 / 2 + 40L * 1000 + 40 * 41 / 2 - 28) + "\n");
        });
    }

    @Test
    public void testPieceMergeWithVarSizeKeysAndColumnTops() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        assertMemoryLeak(() -> {
            createVarSizeKeyTable();
            final TableToken token = engine.verifyTableName("x");
            // The merge writes the piece that holds the day's rows afresh, at the tail of the column files.
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:20', 'zz', 's1', -1, 5, 'q')");
            drainWalQueue();
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(1, reader.getGeometry().getPieceCount(0));
                Assert.assertEquals(reader.getMetadata().getDedupVersion(), reader.getGeometry().getPieceDedupSortVersion(0, 0));
            }
            assertVarSizeKeyOrder();
        });
    }

    @Test
    public void testSplitPartitionKeyOrder() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1024);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, k LONG, s SYMBOL, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE oracle (ts TIMESTAMP, k LONG, s SYMBOL, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("x");
            // Four rows per timestamp with descending keys, written without dedup.
            executeBoth("INSERT INTO %s SELECT dateadd('s', ((x - 1) / 4)::INT, '2024-01-01'::TIMESTAMP), 100_000 - x, 's' || (x % 3), x::INT FROM long_sequence(4000)");
            executeBoth("INSERT INTO %s VALUES ('2024-01-02', 1, 'a', 1)");
            // The oracle stays without dedup: no row below repeats a key, so both tables hold the same rows.
            execute("ALTER TABLE x DEDUP ENABLE UPSERT KEYS(ts, k, s)");
            drainWalQueue();
            assertSortedBits(token, false, false);

            // A row near the end of day one splits it: the rows before it stay where and as they are, the tail
            // is written afresh, in key order.
            executeBoth("INSERT INTO %s VALUES ('2024-01-01T00:16:30', 7, 'a', -1)");
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(3, reader.getPartitionCount());
            }
            assertSortedBits(token, false, true, false);
            assertKeyOrder(token, 1);
            assertOracle();

            // The same on a partition that is in key order: both parts keep the bit.
            executeBoth("INSERT INTO %s SELECT dateadd('s', ((x - 1) / 4)::INT, '2024-01-03'::TIMESTAMP), 100_000 - x, 's' || (x % 3), x::INT FROM long_sequence(4000)");
            executeBoth("INSERT INTO %s VALUES ('2024-01-04', 1, 'a', 1)");
            assertSortedBits(token, false, true, false, true, true);
            executeBoth("INSERT INTO %s VALUES ('2024-01-03T00:16:30', 7, 'a', -1)");
            assertSortedBits(token, false, true, false, true, true, true);
            assertKeyOrder(token, 3);
            assertKeyOrder(token, 4);
            assertOracle();
        });
    }

    private static void assertKeyOrder(TableToken token) {
        try (TableReader reader = engine.getReader(token)) {
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getTxFile().isPartitionDedupSorted(i)) {
                    assertKeyOrder(reader, i);
                }
            }
        }
    }

    private static void assertKeyOrder(TableToken token, int partitionIndex) {
        try (TableReader reader = engine.getReader(token)) {
            Assert.assertTrue(reader.getTxFile().isPartitionDedupSorted(partitionIndex));
            assertKeyOrder(reader, partitionIndex);
        }
    }

    /**
     * Reads the partition's column files in row order: timestamps ascend, and the rows of one timestamp ascend by
     * {@code (k, s)}, a signed integer and the symbol's integer key, as WAL apply compares them.
     */
    private static void assertKeyOrder(TableReader reader, int partitionIndex) {
        final long rowCount = reader.openPartition(partitionIndex);
        Assert.assertTrue(rowCount > 0);
        final int base = reader.getColumnBase(partitionIndex);
        final int kIndex = reader.getMetadata().getColumnIndex("k");
        final boolean isLongKey = reader.getMetadata().getColumnType(kIndex) == ColumnType.LONG;
        final MemoryCR k = reader.getColumn(TableReader.getPrimaryColumnIndex(base, kIndex));
        final int sIndex = reader.getMetadata().getColumnIndexQuiet("s");
        final MemoryCR s = sIndex > -1 ? reader.getColumn(TableReader.getPrimaryColumnIndex(base, sIndex)) : null;
        final MemoryCR ts = reader.getColumn(TableReader.getPrimaryColumnIndex(base, reader.getMetadata().getTimestampIndex()));
        for (long row = 1; row < rowCount; row++) {
            final long prevTs = ts.getLong((row - 1) * Long.BYTES);
            final long thisTs = ts.getLong(row * Long.BYTES);
            Assert.assertTrue(prevTs <= thisTs);
            if (prevTs < thisTs) {
                continue;
            }
            final long prevK = isLongKey ? k.getLong((row - 1) * Long.BYTES) : k.getInt((row - 1) * Integer.BYTES);
            final long thisK = isLongKey ? k.getLong(row * Long.BYTES) : k.getInt(row * Integer.BYTES);
            Assert.assertTrue("partition " + partitionIndex + ", row " + row + ": k " + prevK + " before " + thisK, prevK <= thisK);
            if (prevK == thisK && s != null) {
                Assert.assertTrue(
                        "partition " + partitionIndex + ", row " + row + ": symbol key order",
                        s.getInt((row - 1) * Integer.BYTES) <= s.getInt(row * Integer.BYTES)
                );
            }
        }
    }

    private static void assertSortedBits(TableToken token, boolean... expected) {
        try (TableReader reader = engine.getReader(token)) {
            final StringSink actual = new StringSink();
            final StringSink wanted = new StringSink();
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                actual.put(reader.getTxFile().isPartitionDedupSorted(i) ? '1' : '0');
            }
            for (boolean bit : expected) {
                wanted.put(bit ? '1' : '0');
            }
            TestUtils.assertEquals(wanted, actual);
        }
    }

    /**
     * Reads the first day in storage order and checks that the rows of one timestamp ascend by the dedup keys
     * {@code (k VARCHAR, s STRING, b LONG, k2 VARCHAR)}, compared as WAL apply compares them: NULL first, then a
     * variable-size value by its length and then by its bytes, a number as a signed integer.
     */
    private static void assertVarSizeKeyOrder() throws Exception {
        try (
                RecordCursorFactory factory = select("SELECT ts, k, s, b, k2 FROM x WHERE ts IN '2024-01-01'");
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            long prevTs = Long.MIN_VALUE;
            String prevK = null;
            String prevS = null;
            long prevB = 0;
            String prevK2 = null;
            long tieCount = 0;
            long row = 0;
            while (cursor.hasNext()) {
                final long ts = record.getTimestamp(0);
                final Utf8Sequence kValue = record.getVarcharA(1);
                final String k = kValue != null ? kValue.toString() : null;
                final CharSequence sValue = record.getStrA(2);
                final String s = sValue != null ? sValue.toString() : null;
                final long b = record.getLong(3);
                final Utf8Sequence k2Value = record.getVarcharA(4);
                final String k2 = k2Value != null ? k2Value.toString() : null;
                Assert.assertTrue(prevTs <= ts);
                if (prevTs == ts) {
                    tieCount++;
                    int diff = compareVarSize(prevK, k);
                    if (diff == 0) {
                        diff = compareVarSize(prevS, s);
                    }
                    if (diff == 0) {
                        diff = Long.compare(prevB, b);
                    }
                    if (diff == 0) {
                        diff = compareVarSize(prevK2, k2);
                    }
                    Assert.assertTrue(
                            "row " + row + ": (" + prevK + ", " + prevS + ", " + prevB + ", " + prevK2 + ") before (" + k + ", " + s + ", " + b + ", " + k2 + ')',
                            diff <= 0
                    );
                }
                prevTs = ts;
                prevK = k;
                prevS = s;
                prevB = b;
                prevK2 = k2;
                row++;
            }
            Assert.assertTrue(tieCount > 100);
        }
    }

    // NULL first, then by length, then by content. The values are ASCII, so chars and bytes compare alike.
    private static int compareVarSize(String l, String r) {
        final int lLen = l != null ? l.length() : -1;
        final int rLen = r != null ? r.length() : -1;
        if (lLen != rLen) {
            return Integer.compare(lLen, rLen);
        }
        return lLen > 0 ? Integer.signum(l.compareTo(r)) : 0;
    }

    /**
     * Day one without dedup: 40 timestamps of five rows with VARCHAR and STRING values of several lengths, NULLs
     * among them, in no key order. Then two more columns, so the rows so far sit below their column tops, and 40
     * more rows with values in them. Then dedup on all four.
     */
    private static void createVarSizeKeyTable() throws Exception {
        execute("CREATE TABLE x (ts TIMESTAMP, k VARCHAR, s STRING, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO x SELECT
                    dateadd('s', ((x - 1) / 5)::INT, '2024-01-01'::TIMESTAMP),
                    CASE WHEN x % 7 = 0 THEN NULL WHEN x % 3 = 0 THEN 'a-varchar-longer-than-the-inlined-size-' || (300 - x) ELSE 'v' || (300 - x) END::VARCHAR,
                    CASE WHEN x % 4 = 0 THEN NULL ELSE rpad('s', (x % 3 + 1)::INT, 's') || (9 - x % 9) END::STRING,
                    x::INT
                FROM long_sequence(200)
                """);
        execute("INSERT INTO x VALUES ('2024-01-02', 'a', 'a', 0)");
        drainWalQueue();
        execute("ALTER TABLE x ADD COLUMN b LONG");
        execute("ALTER TABLE x ADD COLUMN k2 VARCHAR");
        execute("""
                INSERT INTO x SELECT
                    dateadd('s', (50 + (x - 1) / 5)::INT, '2024-01-01'::TIMESTAMP),
                    'w' || (x % 2),
                    CASE WHEN x % 4 = 0 THEN NULL ELSE 'a' END::STRING,
                    (x + 1000)::INT,
                    CASE WHEN x % 6 = 0 THEN NULL ELSE 50 - x END,
                    CASE WHEN x % 5 = 0 THEN NULL ELSE 'k' || (50 - x) END::VARCHAR
                FROM long_sequence(40)
                """);
        execute("ALTER TABLE x DEDUP ENABLE UPSERT KEYS(ts, k, s, b, k2)");
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
    }

    private static void executeBoth(String sql) throws Exception {
        execute(sql.replace("%s", "x"));
        execute(sql.replace("%s", "oracle"));
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("oracle")));
    }

    // Two days, each with timestamps that hold several rows, inserted with descending keys.
    private static void insertTwoDays() throws Exception {
        final String sql = """
                INSERT INTO %s VALUES
                ('2024-01-01T00:00:01', 3, 'b', 1),
                ('2024-01-01T00:00:01', 2, 'b', 2),
                ('2024-01-01T00:00:01', 2, 'a', 3),
                ('2024-01-01T00:00:02', 9, 'a', 4),
                ('2024-01-01T00:00:02', 1, 'c', 5),
                ('2024-01-02T00:00:01', 6, 'a', 6),
                ('2024-01-02T00:00:02', 5, 'b', 7),
                ('2024-01-02T00:00:02', 4, 'a', 8)
                """;
        execute(sql.replace("%s", "x"));
        drainWalQueue();
    }

    private void assertOracle() throws Exception {
        // The key order is physical: a query that sorts on the full key reads the same rows from both tables.
        final String query = "SELECT * FROM %s ORDER BY ts, k, s, v";
        final String expected = TestUtils.printSqlToString(engine, sqlExecutionContext, query.replace("%s", "oracle"), new StringSink());
        assertQuery(query.replace("%s", "x")).noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
    }
}
