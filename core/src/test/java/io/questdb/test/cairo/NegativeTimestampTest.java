package io.questdb.test.cairo;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class NegativeTimestampTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(NegativeTimestampTest.class);

    // TODO: DEDUP mode with negative timestamps needs further investigation.
    // The table ends up empty after drainWalQueue(), suggesting the dedup
    // commit-to-timestamp logic has issues with negative timestamps.
    // @Ignore("DEDUP with negative timestamps not yet fully supported") // Temporarily removed for debugging
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
            LOG.info().$("After insert ts1, rowCount=").$(getRowCount(tableName)).$();

            execute("insert into " + tableName + " values (" + ts2 + ", 2)");
            LOG.info().$("After insert ts2, rowCount=").$(getRowCount(tableName)).$();

            // Insert duplicate
            execute("insert into " + tableName + " values (" + ts1 + ", 1)");
            LOG.info().$("After insert duplicate, rowCount=").$(getRowCount(tableName)).$();

            try {
                drainWalQueue();
            } catch (Throwable t) {
                LOG.error().$("drainWalQueue failed: ").$(t).$();
                throw t;
            }
            LOG.info().$("After drainWalQueue, rowCount=").$(getRowCount(tableName)).$();

            // Debug: print actual content
            try (RecordCursorFactory factory = engine.select("select * from " + tableName, sqlExecutionContext);
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                LOG.info().$("Actual rows:").$();
                Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    LOG.info().$("  ts=").$(record.getTimestamp(0)).$(", val=").$(record.getInt(1)).$();
                }
            }

            assertSql("ts\tval\n" +
                    "1969-12-31T23:59:59.000000Z\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t2\n", "select * from " + tableName);
        });
    }

    private long getRowCount(String tableName) throws Exception {
        try (RecordCursorFactory factory = engine.select("select count() from " + tableName, sqlExecutionContext);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            cursor.hasNext();
            return cursor.getRecord().getLong(0);
        }
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

            assertSql("ts\tval\n" +
                    "1969-12-31T23:59:59.000000Z\t2\n" +
                    "1970-01-01T00:00:01.000000Z\t1\n", "select * from " + tableName);
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
            assertSql("ts\tfirst\n" +
                    "1969-12-31T22:00:00.000000Z\t1\n" +
                    "1969-12-31T23:00:00.000000Z\t1\n" +
                    "1970-01-01T00:00:00.000000Z\t2\n",
                    "select ts, first(val) from " + tableName + " sample by 1h fill(prev)");

        });
    }

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

            assertSql("ts\tval\n" +
                    "1900-01-01T00:00:00.000000Z\t1900\n" +
                    "1950-01-01T00:00:00.000000Z\t1950\n", "select * from " + tableName);
        });
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

            assertSql("ts\tval\n" +
                    "1900-01-01T00:00:00.000000Z\t1900\n" +
                    "1950-01-01T00:00:00.000000Z\t1950\n" +
                    "1955-01-01T00:00:00.000000Z\t1955\n" +
                    "1960-01-01T00:00:00.000000Z\t1960\n" +
                    "1970-01-01T00:00:00.000000Z\t1970\n",
                    "select * from " + tableName);
        });
    }

    private void execute(String sql) throws Exception {
        execute(sql, sqlExecutionContext);
    }

    private void execute(String sql, SqlExecutionContext sqlExecutionContext) throws Exception {
        engine.execute(sql, sqlExecutionContext);
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

            System.out.println("Before radix sort:");
            System.out.println("  Array A (LAG raw ts): " + Unsafe.getUnsafe().getLong(aAddr));
            System.out.println("  Array B (WAL indexed): ts=" + Unsafe.getUnsafe().getLong(bAddr)
                    + ", i=" + Unsafe.getUnsafe().getLong(bAddr + Long.BYTES));
            System.out.println("  min=" + min + ", max=" + max);

            // Output goes to aAddr (which has extra space allocated)
            long rowCount = Vect.radixSortABLongIndexAsc(aAddr, countA, bAddr, countB, aAddr, cpyAddr, min, max);

            System.out.println("After radix sort, rowCount=" + rowCount);

            // Check output (indexed format: {ts, i} pairs)
            for (int i = 0; i < rowCount; i++) {
                long ts = Unsafe.getUnsafe().getLong(aAddr + i * 2L * Long.BYTES);
                long idx = Unsafe.getUnsafe().getLong(aAddr + i * 2L * Long.BYTES + Long.BYTES);
                System.out.println("  Output[" + i + "]: ts=" + ts + ", i=" + idx);
            }

            Assert.assertEquals("Row count should match", countA + countB, rowCount);

            // Verify sorted order
            long ts0 = Unsafe.getUnsafe().getLong(aAddr);
            long ts1 = Unsafe.getUnsafe().getLong(aAddr + 2 * Long.BYTES);

            System.out.println("Verifying sorted order: ts0=" + ts0 + ", ts1=" + ts1);
            Assert.assertTrue("Should be sorted: ts0 <= ts1", ts0 <= ts1);
            Assert.assertEquals("ts0 should be -1000000", -1000000L, ts0);
            Assert.assertEquals("ts1 should be 1000000", 1000000L, ts1);

        } finally {
            Unsafe.free(aAddr, sizeA + 2 * Long.BYTES * (countA + 1), MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(bAddr, sizeB, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(cpyAddr, resultSize, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
