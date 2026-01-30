package io.questdb.test.cairo;

import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class NegativeTimestampTest extends AbstractCairoTest {

    @Test
    public void test1900Timestamp() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "test_1900_ts";
            execute("create table " + tableName + " (ts timestamp, val int) timestamp(ts) partition by YEAR WAL");

            long ts1900 = -2208988800000000L; // 1900-01-01T00:00:00.000000Z
            long ts1950 = -631152000000000L; // 1950-01-01T00:00:00.000000Z

            execute("insert into " + tableName + " values (" + ts1900 + ", 1900)");
            execute("insert into " + tableName + " values (" + ts1950 + ", 1950)");

            drainWalQueue();

            assertSql("""
                    ts\tval
                    1900-01-01T00:00:00.000000Z\t1900
                    1950-01-01T00:00:00.000000Z\t1950
                    """, "select * from " + tableName);
        });
    }

    /**
     * Direct test of binary search with negative timestamps.
     */
    @SuppressWarnings({"ConstantValue", "PointlessArithmeticExpression"})
    @Test
    public void testBinarySearchWithNegativeTimestamps() {
        // Create index_t array with sorted timestamps: 1900, 1950, 1960, 1970
        int count = 4;
        int entrySize = 2 * Long.BYTES; // 16 bytes per index_t entry
        long addr = Unsafe.malloc(count * entrySize, MemoryTag.NATIVE_DEFAULT);

        try {
            // Timestamps (sorted order)
            long ts1900 = -2208988800000000L; // 1900-01-01
            long ts1950 = -631152000000000L;  // 1950-01-01
            long ts1960 = -315619200000000L;  // 1960-01-01
            long ts1970 = 0L;                  // 1970-01-01

            // Write sorted index_t entries: {ts, i}
            Unsafe.getUnsafe().putLong(addr + 0 * entrySize, ts1900);
            Unsafe.getUnsafe().putLong(addr + 0 * entrySize + Long.BYTES, 0L);
            Unsafe.getUnsafe().putLong(addr + 1 * entrySize, ts1950);
            Unsafe.getUnsafe().putLong(addr + 1 * entrySize + Long.BYTES, 1L);
            Unsafe.getUnsafe().putLong(addr + 2 * entrySize, ts1960);
            Unsafe.getUnsafe().putLong(addr + 2 * entrySize + Long.BYTES, 2L);
            Unsafe.getUnsafe().putLong(addr + 3 * entrySize, ts1970);
            Unsafe.getUnsafe().putLong(addr + 3 * entrySize + Long.BYTES, 3L);

            // Ceiling for 1900 partition (end of year 1900)
            long ceilFor1900 = Micros.ceilYYYY(ts1900) - 1;

            // Verify that only ts1900 is <= ceiling
            Assert.assertTrue(ts1900 <= ceilFor1900);
            Assert.assertFalse(ts1950 <= ceilFor1900);
            Assert.assertFalse(ts1960 <= ceilFor1900);
            Assert.assertFalse(ts1970 <= ceilFor1900);

            // Binary search should find only index 0 (the 1900 timestamp)
            long result = Vect.boundedBinarySearchIndexT(addr, ceilFor1900, 0, count - 1, Vect.BIN_SEARCH_SCAN_DOWN);
            Assert.assertEquals("Binary search should return 0 (only 1900 is <= ceiling)", 0, result);
        } finally {
            Unsafe.free(addr, count * entrySize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testComplexO3NegativeTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "test_complex_o3_negative";
            execute("create table " + tableName + " (ts timestamp, val int) timestamp(ts) partition by YEAR WAL");

            // 1. Initial insert (e.g. 1960)
            long ts1960 = -315619200000000L; // 1960-01-01
            execute("insert into " + tableName + " values (" + ts1960 + ", 1960)");

            // 2. O3 insert (older, e.g. 1950) -> Should trigger O3 merge in 1950 partition
            // (or create it)
            long ts1950 = -631152000000000L; // 1950-01-01
            execute("insert into " + tableName + " values (" + ts1950 + ", 1950)");

            // 3. O3 insert (even older, e.g. 1900)
            long ts1900 = -2208988800000000L; // 1900-01-01
            execute("insert into " + tableName + " values (" + ts1900 + ", 1900)");

            // 4. O3 insert (newer than initial, e.g. 1970)
            long ts1970 = 0L; // 1970-01-01
            execute("insert into " + tableName + " values (" + ts1970 + ", 1970)");

            // 5. O3 insert (between 1950 and 1960)
            long ts1955 = -473385600000000L; // 1955-01-01
            execute("insert into " + tableName + " values (" + ts1955 + ", 1955)");

            drainWalQueue();

            assertSql("""
                            ts\tval
                            1900-01-01T00:00:00.000000Z\t1900
                            1950-01-01T00:00:00.000000Z\t1950
                            1955-01-01T00:00:00.000000Z\t1955
                            1960-01-01T00:00:00.000000Z\t1960
                            1970-01-01T00:00:00.000000Z\t1970
                            """,
                    "select * from " + tableName);
        });
    }

    @Test
    public void testFillRangeNegative() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "test_fill_negative";
            execute("create table " + tableName + " (ts timestamp, val int) timestamp(ts) partition by DAY");
            execute("insert into " + tableName + " values (-7200000000, 1)"); // -2 hours
            execute("insert into " + tableName + " values (0, 2)");

            // SAMPLE BY 1h FILL(PREV)
            // Should fill the gap at -1h
            assertSql("""
                            ts\tfirst
                            1969-12-31T22:00:00.000000Z\t1
                            1969-12-31T23:00:00.000000Z\t1
                            1970-01-01T00:00:00.000000Z\t2
                            """,
                    "select ts, first(val) from " + tableName + " sample by 1h fill(prev)");

        });
    }

    @Test
    public void testNegativeTimestampDeduplication() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "test_negative_ts_dedup";
            execute("create table " + tableName
                    + " (ts timestamp, val int) timestamp(ts) partition by DAY WAL DEDUP UPSERT KEYS(ts, val)");

            long ts1 = -1000000L; // Before 1970
            long ts2 = 1000000L; // After 1970

            // Insert mixed positive and negative timestamps
            execute("insert into " + tableName + " values (" + ts1 + ", 1)");
            execute("insert into " + tableName + " values (" + ts2 + ", 2)");

            // Insert duplicate
            execute("insert into " + tableName + " values (" + ts1 + ", 1)");

            drainWalQueue();

            assertSql("""
                    ts\tval
                    1969-12-31T23:59:59.000000Z\t1
                    1970-01-01T00:00:01.000000Z\t2
                    """, "select * from " + tableName);
        });
    }

    @Test
    public void testO3InsertNegativeTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "test_negative_ts_o3";
            execute("create table " + tableName + " (ts timestamp, val int) timestamp(ts) partition by DAY WAL");

            // Insert initial row (positive)
            execute("insert into " + tableName + " values (1000000, 1)");

            // O3 insert (negative)
            execute("insert into " + tableName + " values (-1000000, 2)");

            drainWalQueue();

            assertSql("""
                    ts\tval
                    1969-12-31T23:59:59.000000Z\t2
                    1970-01-01T00:00:01.000000Z\t1
                    """, "select * from " + tableName);
        });
    }

    /**
     * Direct test of radixSortABLongIndexAsc with negative timestamps.
     * This isolates the radix sort behavior from the full DEDUP flow.
     */
    @Test
    public void testRadixSortABWithNegativeTimestamps() throws Exception {
        // Test case: LAG has ts=1000000, WAL has ts=-1000000
        // Expected output: sorted as [ts=-1000000, ts=1000000]

        // Array A (LAG): raw timestamps (8 bytes each)
        int countA = 1;
        int sizeA = countA * Long.BYTES;
        long aAddr = Unsafe.malloc(sizeA + 2 * Long.BYTES * (countA + 1), MemoryTag.NATIVE_DEFAULT); // Extra space for output

        // Array B (WAL): indexed timestamps (16 bytes each: {ts, rowIndex})
        int countB = 1;
        int sizeB = countB * 2 * Long.BYTES;
        long bAddr = Unsafe.malloc(sizeB, MemoryTag.NATIVE_DEFAULT);

        // Copy buffer
        int resultSize = (countA + countB) * 2 * Long.BYTES;
        long cpyAddr = Unsafe.malloc(resultSize, MemoryTag.NATIVE_DEFAULT);

        try {
            // Seed Array A (LAG): raw timestamp = 1000000
            Unsafe.getUnsafe().putLong(aAddr, 1000000L);

            // Seed Array B (WAL): indexed timestamp = {ts: -1000000, rowIndex: 0}
            Unsafe.getUnsafe().putLong(bAddr, -1000000L);        // ts
            Unsafe.getUnsafe().putLong(bAddr + Long.BYTES, 0L);  // rowIndex

            long min = -1000000L;
            long max = 1000000L;

            // Output goes to aAddr (which has extra space allocated)
            long rowCount = Vect.radixSortABLongIndexAsc(aAddr, countA, bAddr, countB, aAddr, cpyAddr, min, max);

            Assert.assertEquals("Row count should match", countA + countB, rowCount);

            // Verify sorted order
            long ts0 = Unsafe.getUnsafe().getLong(aAddr);
            long ts1 = Unsafe.getUnsafe().getLong(aAddr + 2 * Long.BYTES);

            Assert.assertTrue("Should be sorted: ts0 <= ts1", ts0 <= ts1);
            Assert.assertEquals("ts0 should be -1000000", -1000000L, ts0);
            Assert.assertEquals("ts1 should be 1000000", 1000000L, ts1);

        } finally {
            Unsafe.free(aAddr, sizeA + 2 * Long.BYTES * (countA + 1), MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(bAddr, sizeB, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(cpyAddr, resultSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void execute(String sql) throws Exception {
        execute(sql, sqlExecutionContext);
    }

    private void execute(String sql, SqlExecutionContext sqlExecutionContext) throws Exception {
        engine.execute(sql, sqlExecutionContext);
    }
}
