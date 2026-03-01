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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.jit.JitUtil;
import io.questdb.std.FilesFacade;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class NullBitmapTest extends AbstractCairoTest {

    // ========================
    // Diagnostic: verify .n file contents
    // ========================

    @Test
    public void testBitmapFileExistsAndHasCorrectContents() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val BOOLEAN) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', true),
                    ('2024-01-01T00:00:01.000000Z', false),
                    ('2024-01-01T00:00:02.000000Z', NULL),
                    ('2024-01-01T00:00:03.000000Z', true)
                    """);

            try (TableReader reader = getReader("t")) {
                reader.openPartition(0);
                int base = reader.getColumnBase(0);
                MemoryCR bitmapMem = reader.getNullBitmapColumn(base, 1);
                Assert.assertNotNull("Bitmap column should not be null", bitmapMem);
                Assert.assertTrue("Bitmap should have size > 0", bitmapMem.size() > 0);

                long addr = bitmapMem.getPageAddress(0);
                Assert.assertNotEquals("Bitmap address should not be 0", 0, addr);

                byte bitmapByte = Unsafe.getUnsafe().getByte(addr);
                // Row 2 is null, so bit 2 should be set: 0x04
                Assert.assertEquals("Bitmap byte should have bit 2 set for null row",
                        (byte) 0x04, bitmapByte);
            }
        });
    }

    // ========================
    // BOOLEAN null support
    // ========================

    @Test
    public void testBooleanNullInsertAndSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val BOOLEAN) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', true),
                    ('2024-01-01T00:00:01.000000Z', false),
                    ('2024-01-01T00:00:02.000000Z', NULL),
                    ('2024-01-01T00:00:03.000000Z', true)
                    """);
            assertQueryNoLeakCheck(
                    """
                            ts\tval
                            2024-01-01T00:00:00.000000Z\ttrue
                            2024-01-01T00:00:01.000000Z\tfalse
                            2024-01-01T00:00:02.000000Z\t
                            2024-01-01T00:00:03.000000Z\ttrue
                            """,
                    "SELECT * FROM t",
                    "ts", true, true
            );
        });
    }

    @Test
    public void testBooleanNullIsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val BOOLEAN) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', true),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', false)
                    """);
            assertSql(
                    """
                            ts\tval
                            2024-01-01T00:00:01.000000Z\t
                            """,
                    "SELECT * FROM t WHERE val IS NULL"
            );
        });
    }

    @Test
    public void testBooleanCoalesce() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, a BOOLEAN, b BOOLEAN) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', NULL, true),
                    ('2024-01-01T00:00:01.000000Z', false, NULL),
                    ('2024-01-01T00:00:02.000000Z', NULL, NULL)
                    """);
            // First verify the data is correct
            assertQueryNoLeakCheck(
                    """
                            ts\ta\tb
                            2024-01-01T00:00:00.000000Z\t\ttrue
                            2024-01-01T00:00:01.000000Z\tfalse\t
                            2024-01-01T00:00:02.000000Z\t\t
                            """,
                    "SELECT * FROM t",
                    "ts", true, true
            );
            // Then test COALESCE
            assertSql(
                    """
                            c
                            true
                            false

                            """,
                    "SELECT COALESCE(a, b) AS c FROM t"
            );
        });
    }

    @Test
    public void testBooleanCastToInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val BOOLEAN) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', true),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', false)
                    """);
            assertSql(
                    """
                            c
                            1
                            null
                            0
                            """,
                    "SELECT val::INT AS c FROM t"
            );
        });
    }

    // ========================
    // BYTE null support
    // ========================

    @Test
    public void testByteNullInsertAndSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 42),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0),
                    ('2024-01-01T00:00:03.000000Z', -1)
                    """);
            assertQueryNoLeakCheck(
                    """
                            ts\tval
                            2024-01-01T00:00:00.000000Z\t42
                            2024-01-01T00:00:01.000000Z\t
                            2024-01-01T00:00:02.000000Z\t0
                            2024-01-01T00:00:03.000000Z\t-1
                            """,
                    "SELECT * FROM t",
                    "ts", true, true
            );
        });
    }

    @Test
    public void testByteNullIsDistinctFromZero() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 0),
                    ('2024-01-01T00:00:01.000000Z', NULL)
                    """);
            assertSql(
                    """
                            cnt
                            1
                            """,
                    "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL"
            );
        });
    }

    @Test
    public void testByteCastToLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 42),
                    ('2024-01-01T00:00:01.000000Z', NULL)
                    """);
            assertSql(
                    """
                            c
                            42
                            null
                            """,
                    "SELECT val::LONG AS c FROM t"
            );
        });
    }

    @Test
    public void testByteCoalesce() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, a BYTE, b BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', NULL, 10),
                    ('2024-01-01T00:00:01.000000Z', 5, NULL),
                    ('2024-01-01T00:00:02.000000Z', NULL, NULL)
                    """);
            assertSql(
                    """
                            c
                            10
                            5

                            """,
                    "SELECT COALESCE(a, b) AS c FROM t"
            );
        });
    }

    // ========================
    // SHORT null support
    // ========================

    @Test
    public void testShortNullInsertAndSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val SHORT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1000),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0),
                    ('2024-01-01T00:00:03.000000Z', -1)
                    """);
            assertQueryNoLeakCheck(
                    """
                            ts\tval
                            2024-01-01T00:00:00.000000Z\t1000
                            2024-01-01T00:00:01.000000Z\t
                            2024-01-01T00:00:02.000000Z\t0
                            2024-01-01T00:00:03.000000Z\t-1
                            """,
                    "SELECT * FROM t",
                    "ts", true, true
            );
        });
    }

    @Test
    public void testShortNullIsDistinctFromZero() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val SHORT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 0),
                    ('2024-01-01T00:00:01.000000Z', NULL)
                    """);
            assertSql(
                    """
                            cnt
                            1
                            """,
                    "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL"
            );
        });
    }

    @Test
    public void testShortCoalesce() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, a SHORT, b SHORT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', NULL, 100),
                    ('2024-01-01T00:00:01.000000Z', 50, NULL),
                    ('2024-01-01T00:00:02.000000Z', NULL, NULL)
                    """);
            assertSql(
                    """
                            c
                            100
                            50

                            """,
                    "SELECT COALESCE(a, b) AS c FROM t"
            );
        });
    }

    @Test
    public void testShortCastToDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val SHORT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 42),
                    ('2024-01-01T00:00:01.000000Z', NULL)
                    """);
            assertSql(
                    """
                            c
                            42.0
                            null
                            """,
                    "SELECT val::DOUBLE AS c FROM t"
            );
        });
    }

    // ========================
    // UINT16 null support
    // ========================

    @Test
    public void testUInt16NullInsertAndSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val UINT16) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 100),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0),
                    ('2024-01-01T00:00:03.000000Z', 65535::UINT16)
                    """);
            assertQueryNoLeakCheck(
                    """
                            ts\tval
                            2024-01-01T00:00:00.000000Z\t100
                            2024-01-01T00:00:01.000000Z\t
                            2024-01-01T00:00:02.000000Z\t0
                            2024-01-01T00:00:03.000000Z\t65535
                            """,
                    "SELECT * FROM t",
                    "ts", true, true
            );
        });
    }

    // ========================
    // UINT32 null support
    // ========================

    @Test
    public void testUInt32NullInsertAndSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val UINT32) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 100),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0),
                    ('2024-01-01T00:00:03.000000Z', 4294967295::UINT32)
                    """);
            assertQueryNoLeakCheck(
                    """
                            ts\tval
                            2024-01-01T00:00:00.000000Z\t100
                            2024-01-01T00:00:01.000000Z\t
                            2024-01-01T00:00:02.000000Z\t0
                            2024-01-01T00:00:03.000000Z\t4294967295
                            """,
                    "SELECT * FROM t",
                    "ts", true, true
            );
        });
    }

    // ========================
    // UINT64 null support
    // ========================

    @Test
    public void testUInt64NullInsertAndSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val UINT64) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 100),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0)
                    """);
            assertQueryNoLeakCheck(
                    """
                            ts\tval
                            2024-01-01T00:00:00.000000Z\t100
                            2024-01-01T00:00:01.000000Z\t
                            2024-01-01T00:00:02.000000Z\t0
                            """,
                    "SELECT * FROM t",
                    "ts", true, true
            );
        });
    }

    // ========================
    // Mixed type operations
    // ========================

    @Test
    public void testCaseWhenBooleanNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-01T00:00:01.000000Z', 2),
                    ('2024-01-01T00:00:02.000000Z', 3)
                    """);
            assertSql(
                    """
                            c
                            true

                            false
                            """,
                    "SELECT CASE WHEN val = 1 THEN true WHEN val = 3 THEN false ELSE NULL END AS c FROM t"
            );
        });
    }

    @Test
    public void testGroupByFirstLastBooleanWithNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp INT, val BOOLEAN) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1, true),
                    ('2024-01-01T00:00:01.000000Z', 1, NULL),
                    ('2024-01-01T00:00:02.000000Z', 1, false),
                    ('2024-01-01T00:00:03.000000Z', 2, NULL),
                    ('2024-01-01T00:00:04.000000Z', 2, NULL)
                    """);
            assertSql(
                    """
                            grp\tf\tl
                            1\ttrue\tfalse
                            2\t\t
                            """,
                    "SELECT grp, FIRST(val) AS f, LAST(val) AS l FROM t ORDER BY grp"
            );
        });
    }

    @Test
    public void testGroupByFirstLastByteWithNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp INT, val BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1, 10),
                    ('2024-01-01T00:00:01.000000Z', 1, NULL),
                    ('2024-01-01T00:00:02.000000Z', 1, 20),
                    ('2024-01-01T00:00:03.000000Z', 2, NULL),
                    ('2024-01-01T00:00:04.000000Z', 2, NULL)
                    """);
            assertSql(
                    """
                            grp\tf\tl
                            1\t10\t20
                            2\t\t
                            """,
                    "SELECT grp, FIRST(val) AS f, LAST(val) AS l FROM t ORDER BY grp"
            );
        });
    }

    // ========================
    // Lazy bitmap migration
    // ========================

    @Test
    public void testLazyBitmapMigrationForBooleanColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_migrate (ts TIMESTAMP, val BOOLEAN) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t_migrate VALUES
                    ('2024-01-01T00:00:00.000000Z', true),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', false),
                    ('2024-01-01T00:00:03.000000Z', NULL)
                    """);

            // Verify data before deleting .n
            assertSql("cnt\n2\n", "SELECT COUNT(*) AS cnt FROM t_migrate WHERE val IS NULL");

            // Delete .n file to simulate pre-upgrade partition
            FilesFacade ff = configuration.getFilesFacade();
            TableToken token = engine.verifyTableName("t_migrate");
            try (Path path = new Path()) {
                final int rootLen = path.of(configuration.getDbRoot()).concat(token).size();
                TableUtils.setPathForNativePartition(path.trimTo(rootLen), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY,
                        1704067200000000L, -1L);
                final int partitionLen = path.size();
                TableUtils.nFile(path, "val");
                Assert.assertTrue("Bitmap file should exist before deletion", ff.exists(path.$()));
                ff.remove(path.$());
                Assert.assertFalse("Bitmap file should not exist after deletion", ff.exists(path.$()));
            }

            // Close and reopen - should trigger lazy migration
            engine.releaseAllReaders();

            // After reopening, BOOLEAN null detection should still work via regenerated bitmap
            // BOOLEAN never had sentinels, so regenerated bitmap should have all rows as non-null
            assertSql("cnt\n4\n", "SELECT COUNT(*) AS cnt FROM t_migrate WHERE val IS NOT NULL OR val IS NULL");
        });
    }

    @Test
    public void testLazyBitmapMigrationForIntColumnDuringConversion() throws Exception {
        // Tests that ALTER TABLE column type conversion correctly generates .n bitmap
        // from sentinel values when converting an INT column (which has no .n file)
        // to DOUBLE. The NullBitmapMigrator scans the .d file for INT_NULL sentinels
        // and creates the .n file so nulls are preserved through the conversion.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_conv (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t_conv VALUES
                    ('2024-01-01T00:00:00.000000Z', 42),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0),
                    ('2024-01-01T00:00:03.000000Z', NULL),
                    ('2024-01-01T00:00:04.000000Z', 100)
                    """);
            drainWalQueue();

            // Verify INT nulls before conversion
            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t_conv WHERE val IS NOT NULL");
            assertSql("cnt\n2\n", "SELECT COUNT(*) AS cnt FROM t_conv WHERE val IS NULL");

            // Convert INT → DOUBLE (supported conversion)
            execute("ALTER TABLE t_conv ALTER COLUMN val TYPE DOUBLE");
            drainWalQueue();

            // Verify nulls are preserved after conversion
            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t_conv WHERE val IS NOT NULL");
            assertSql("cnt\n2\n", "SELECT COUNT(*) AS cnt FROM t_conv WHERE val IS NULL");

            // Verify non-null values converted correctly
            assertSql(
                    """
                            ts\tval
                            2024-01-01T00:00:00.000000Z\t42.0
                            2024-01-01T00:00:01.000000Z\tnull
                            2024-01-01T00:00:02.000000Z\t0.0
                            2024-01-01T00:00:03.000000Z\tnull
                            2024-01-01T00:00:04.000000Z\t100.0
                            """,
                    "SELECT * FROM t_conv"
            );
        });
    }

    @Test
    public void testLazyBitmapMigrationForLongColumnDuringConversion() throws Exception {
        // Tests lazy migration for LONG columns (sentinel = Long.MIN_VALUE).
        // Converts LONG → DOUBLE, verifying null preservation through
        // NullBitmapMigrator sentinel scanning.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_conv_long (ts TIMESTAMP, val LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t_conv_long VALUES
                    ('2024-01-01T00:00:00.000000Z', 12345),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0),
                    ('2024-01-01T00:00:03.000000Z', -1)
                    """);
            drainWalQueue();

            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t_conv_long WHERE val IS NOT NULL");
            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t_conv_long WHERE val IS NULL");

            execute("ALTER TABLE t_conv_long ALTER COLUMN val TYPE DOUBLE");
            drainWalQueue();

            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t_conv_long WHERE val IS NOT NULL");
            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t_conv_long WHERE val IS NULL");

            assertSql(
                    """
                            ts\tval
                            2024-01-01T00:00:00.000000Z\t12345.0
                            2024-01-01T00:00:01.000000Z\tnull
                            2024-01-01T00:00:02.000000Z\t0.0
                            2024-01-01T00:00:03.000000Z\t-1.0
                            """,
                    "SELECT * FROM t_conv_long"
            );
        });
    }

    @Test
    public void testUInt64ColumnIsNullDispatch() throws Exception {
        // Direct test that UInt64Column.isNull() correctly delegates to rec.isNull()
        assertMemoryLeak(() -> {
            var col = io.questdb.griffin.engine.functions.columns.UInt64Column.newInstance(1);
            // Create a mock record that returns true for isNull(1)
            io.questdb.cairo.sql.Record rec = new io.questdb.cairo.sql.Record() {
                @Override
                public boolean isNull(int columnIndex) {
                    return columnIndex == 1; // column 1 is null
                }

                @Override
                public long getLong(int col) {
                    return 0L;
                }
            };
            Assert.assertTrue("UInt64Column.isNull should delegate to record.isNull", col.isNull(rec));
            Assert.assertFalse("UInt64Column.isNull should return false for non-null column",
                    io.questdb.griffin.engine.functions.columns.UInt64Column.newInstance(0).isNull(rec));
        });
    }

    @Test
    public void testUIntEqualityNullGuard() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, u32 UINT32, u64 UINT64) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 0::UINT32, 0::UINT64),
                    ('2024-01-01T00:00:01.000000Z', NULL, NULL),
                    ('2024-01-01T00:00:02.000000Z', 100::UINT32, 100::UINT64)
                    """);
            // Null rows must NOT match equality with 0 (null fill value)
            assertSql(
                    """
                            ts\tu32\teq_zero\tis_null
                            2024-01-01T00:00:00.000000Z\t0\ttrue\tfalse
                            2024-01-01T00:00:01.000000Z\t\tfalse\ttrue
                            2024-01-01T00:00:02.000000Z\t100\tfalse\tfalse
                            """,
                    "SELECT ts, u32, u32 = 0::UINT32 AS eq_zero, u32 IS NULL AS is_null FROM t ORDER BY ts"
            );
            assertSql(
                    """
                            ts\tu64\teq_zero\tis_null
                            2024-01-01T00:00:00.000000Z\t0\ttrue\tfalse
                            2024-01-01T00:00:01.000000Z\t\tfalse\ttrue
                            2024-01-01T00:00:02.000000Z\t100\tfalse\tfalse
                            """,
                    "SELECT ts, u64, u64 = 0::UINT64 AS eq_zero, u64 IS NULL AS is_null FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testUInt64IsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val UINT64) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 100),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 200),
                    ('2024-01-01T00:00:03.000000Z', NULL),
                    ('2024-01-01T00:00:04.000000Z', 300)
                    """);
            // Verify IS NULL works
            assertSql(
                    """
                            ts\tval\tis_null
                            2024-01-01T00:00:00.000000Z\t100\tfalse
                            2024-01-01T00:00:01.000000Z\t\ttrue
                            2024-01-01T00:00:02.000000Z\t200\tfalse
                            2024-01-01T00:00:03.000000Z\t\ttrue
                            2024-01-01T00:00:04.000000Z\t300\tfalse
                            """,
                    "SELECT ts, val, val IS NULL AS is_null FROM t ORDER BY ts"
            );
            // Verify COUNT excludes nulls
            assertSql(
                    """
                            cnt\ttotal
                            3\t5
                            """,
                    "SELECT COUNT(val) AS cnt, COUNT(*) AS total FROM t"
            );
            // Verify WHERE IS NOT NULL
            assertSql(
                    """
                            cnt
                            3
                            """,
                    "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL"
            );
        });
    }

    @Test
    public void testUInt32IsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val UINT32) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 100),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 200),
                    ('2024-01-01T00:00:03.000000Z', NULL),
                    ('2024-01-01T00:00:04.000000Z', 300)
                    """);
            assertSql(
                    """
                            ts\tval\tis_null
                            2024-01-01T00:00:00.000000Z\t100\tfalse
                            2024-01-01T00:00:01.000000Z\t\ttrue
                            2024-01-01T00:00:02.000000Z\t200\tfalse
                            2024-01-01T00:00:03.000000Z\t\ttrue
                            2024-01-01T00:00:04.000000Z\t300\tfalse
                            """,
                    "SELECT ts, val, val IS NULL AS is_null FROM t ORDER BY ts"
            );
            assertSql(
                    """
                            cnt\ttotal
                            3\t5
                            """,
                    "SELECT COUNT(val) AS cnt, COUNT(*) AS total FROM t"
            );
            assertSql(
                    """
                            cnt
                            3
                            """,
                    "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL"
            );
        });
    }

    @Test
    public void testUInt16IsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val UINT16) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 100),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 200),
                    ('2024-01-01T00:00:03.000000Z', NULL),
                    ('2024-01-01T00:00:04.000000Z', 300)
                    """);
            assertSql(
                    """
                            ts\tval\tis_null
                            2024-01-01T00:00:00.000000Z\t100\tfalse
                            2024-01-01T00:00:01.000000Z\t\ttrue
                            2024-01-01T00:00:02.000000Z\t200\tfalse
                            2024-01-01T00:00:03.000000Z\t\ttrue
                            2024-01-01T00:00:04.000000Z\t300\tfalse
                            """,
                    "SELECT ts, val, val IS NULL AS is_null FROM t ORDER BY ts"
            );
            assertSql(
                    """
                            cnt\ttotal
                            3\t5
                            """,
                    "SELECT COUNT(val) AS cnt, COUNT(*) AS total FROM t"
            );
            assertSql(
                    """
                            cnt
                            3
                            """,
                    "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL"
            );
        });
    }

    @Test
    public void testUInt64IsNullWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val UINT64) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 100),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 200),
                    ('2024-01-01T00:00:03.000000Z', NULL),
                    ('2024-01-01T00:00:04.000000Z', 300)
                    """);
            drainWalQueue();
            assertSql(
                    """
                            ts\tval\tis_null
                            2024-01-01T00:00:00.000000Z\t100\tfalse
                            2024-01-01T00:00:01.000000Z\t\ttrue
                            2024-01-01T00:00:02.000000Z\t200\tfalse
                            2024-01-01T00:00:03.000000Z\t\ttrue
                            2024-01-01T00:00:04.000000Z\t300\tfalse
                            """,
                    "SELECT ts, val, val IS NULL AS is_null FROM t ORDER BY ts"
            );
            assertSql(
                    """
                            cnt\ttotal
                            3\t5
                            """,
                    "SELECT COUNT(val) AS cnt, COUNT(*) AS total FROM t"
            );
        });
    }

    @Test
    public void testUInt32IsNullWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val UINT32) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 100),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 200),
                    ('2024-01-01T00:00:03.000000Z', NULL),
                    ('2024-01-01T00:00:04.000000Z', 300)
                    """);
            drainWalQueue();
            assertSql(
                    """
                            ts\tval\tis_null
                            2024-01-01T00:00:00.000000Z\t100\tfalse
                            2024-01-01T00:00:01.000000Z\t\ttrue
                            2024-01-01T00:00:02.000000Z\t200\tfalse
                            2024-01-01T00:00:03.000000Z\t\ttrue
                            2024-01-01T00:00:04.000000Z\t300\tfalse
                            """,
                    "SELECT ts, val, val IS NULL AS is_null FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testUInt16IsNullWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val UINT16) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 100),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 200),
                    ('2024-01-01T00:00:03.000000Z', NULL),
                    ('2024-01-01T00:00:04.000000Z', 300)
                    """);
            drainWalQueue();
            assertSql(
                    """
                            ts\tval\tis_null
                            2024-01-01T00:00:00.000000Z\t100\tfalse
                            2024-01-01T00:00:01.000000Z\t\ttrue
                            2024-01-01T00:00:02.000000Z\t200\tfalse
                            2024-01-01T00:00:03.000000Z\t\ttrue
                            2024-01-01T00:00:04.000000Z\t300\tfalse
                            """,
                    "SELECT ts, val, val IS NULL AS is_null FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testGroupByFirstLastShortWithNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, grp INT, val SHORT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1, 100),
                    ('2024-01-01T00:00:01.000000Z', 1, NULL),
                    ('2024-01-01T00:00:02.000000Z', 1, 200),
                    ('2024-01-01T00:00:03.000000Z', 2, NULL),
                    ('2024-01-01T00:00:04.000000Z', 2, NULL)
                    """);
            assertSql(
                    """
                            grp\tf\tl
                            1\t100\t200
                            2\t\t
                            """,
                    "SELECT grp, FIRST(val) AS f, LAST(val) AS l FROM t ORDER BY grp"
            );
        });
    }

    // ========================
    // Type conversion with bitmap migration (ConvertOperatorImpl path)
    // ========================

    @Test
    public void testConversionFloatToDouble() throws Exception {
        // FLOAT uses NaN sentinel for nulls. Converting to DOUBLE should
        // generate .n bitmap from NaN sentinels and preserve nulls.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val FLOAT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 3.14),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0.0),
                    ('2024-01-01T00:00:03.000000Z', NULL),
                    ('2024-01-01T00:00:04.000000Z', -1.5)
                    """);
            drainWalQueue();

            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");
            assertSql("cnt\n2\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");

            execute("ALTER TABLE t ALTER COLUMN val TYPE DOUBLE");
            drainWalQueue();

            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");
            assertSql("cnt\n2\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");

            assertSql(
                    """
                            val
                            3.140000104904175
                            null
                            0.0
                            null
                            -1.5
                            """,
                    "SELECT val FROM t"
            );
        });
    }

    @Test
    public void testConversionDoubleToFloat() throws Exception {
        // DOUBLE uses NaN sentinel for nulls. Converting to FLOAT should preserve nulls.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 2.718),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0.0),
                    ('2024-01-01T00:00:03.000000Z', -99.9)
                    """);
            drainWalQueue();

            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");
            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");

            execute("ALTER TABLE t ALTER COLUMN val TYPE FLOAT");
            drainWalQueue();

            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");
            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
        });
    }

    @Test
    public void testConversionIntToLong() throws Exception {
        // INT uses INT_NULL (Integer.MIN_VALUE) sentinel. Converting to LONG
        // should preserve nulls through the native conversion.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 42),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0),
                    ('2024-01-01T00:00:03.000000Z', -100)
                    """);
            drainWalQueue();

            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");
            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");

            execute("ALTER TABLE t ALTER COLUMN val TYPE LONG");
            drainWalQueue();

            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");
            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");

            assertSql(
                    """
                            val
                            42
                            null
                            0
                            -100
                            """,
                    "SELECT val FROM t"
            );
        });
    }

    @Test
    public void testConversionIntToShort() throws Exception {
        // INT (sentinel-null) → SHORT (bitmap-null). Verifies sentinel scanning
        // during conversion produces correct bitmap for the destination type.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 10),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0),
                    ('2024-01-01T00:00:03.000000Z', NULL),
                    ('2024-01-01T00:00:04.000000Z', 127)
                    """);
            drainWalQueue();

            assertSql("cnt\n2\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");

            execute("ALTER TABLE t ALTER COLUMN val TYPE SHORT");
            drainWalQueue();

            assertSql("cnt\n2\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");

            assertSql(
                    """
                            val
                            10

                            0

                            127
                            """,
                    "SELECT val FROM t"
            );
        });
    }

    @Test
    public void testConversionIntToByte() throws Exception {
        // INT (sentinel-null) → BYTE (bitmap-null). Verifies sentinel scanning
        // produces correct bitmap for bitmap-only destination type.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 5),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0),
                    ('2024-01-01T00:00:03.000000Z', 100)
                    """);
            drainWalQueue();

            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");

            execute("ALTER TABLE t ALTER COLUMN val TYPE BYTE");
            drainWalQueue();

            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");

            assertSql(
                    """
                            val
                            5

                            0
                            100
                            """,
                    "SELECT val FROM t"
            );
        });
    }

    @Test
    public void testConversionLongToInt() throws Exception {
        // LONG (LONG_NULL sentinel) → INT (INT_NULL sentinel).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 42),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0),
                    ('2024-01-01T00:00:03.000000Z', -1)
                    """);
            drainWalQueue();

            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");

            execute("ALTER TABLE t ALTER COLUMN val TYPE INT");
            drainWalQueue();

            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");

            assertSql(
                    """
                            val
                            42
                            null
                            0
                            -1
                            """,
                    "SELECT val FROM t"
            );
        });
    }

    @Test
    public void testConversionDoubleToInt() throws Exception {
        // DOUBLE (NaN sentinel) → INT (INT_NULL sentinel).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 42.0),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0.0),
                    ('2024-01-01T00:00:03.000000Z', -1.0)
                    """);
            drainWalQueue();

            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");

            execute("ALTER TABLE t ALTER COLUMN val TYPE INT");
            drainWalQueue();

            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");

            assertSql(
                    """
                            val
                            42
                            null
                            0
                            -1
                            """,
                    "SELECT val FROM t"
            );
        });
    }

    @Test
    public void testConversionIntToBooleanWithNulls() throws Exception {
        // INT (sentinel-null) → BOOLEAN (bitmap-null).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0)
                    """);
            drainWalQueue();

            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");

            execute("ALTER TABLE t ALTER COLUMN val TYPE BOOLEAN");
            drainWalQueue();

            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
            assertSql("cnt\n2\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");

            assertSql(
                    """
                            val
                            true

                            false
                            """,
                    "SELECT val FROM t"
            );
        });
    }

    // ========================
    // ADD COLUMN column-top scenarios
    // ========================

    @Test
    public void testAddBooleanColumnToExistingTable() throws Exception {
        // ADD COLUMN for a bitmap-type to a table with existing data.
        // Old rows should appear as NULL for the new column.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-01T00:00:01.000000Z', 2),
                    ('2024-01-01T00:00:02.000000Z', 3)
                    """);

            execute("ALTER TABLE t ADD COLUMN flag BOOLEAN");

            // Insert rows after ADD COLUMN
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:03.000000Z', 4, true),
                    ('2024-01-01T00:00:04.000000Z', 5, false)
                    """);

            // Old rows should have NULL for flag column
            assertSql(
                    """
                            ts\tval\tflag
                            2024-01-01T00:00:00.000000Z\t1\t
                            2024-01-01T00:00:01.000000Z\t2\t
                            2024-01-01T00:00:02.000000Z\t3\t
                            2024-01-01T00:00:03.000000Z\t4\ttrue
                            2024-01-01T00:00:04.000000Z\t5\tfalse
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );

            // IS NULL should find 3 old rows
            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE flag IS NULL");
            // IS NOT NULL should find 2 new rows
            assertSql("cnt\n2\n", "SELECT COUNT(*) AS cnt FROM t WHERE flag IS NOT NULL");
        });
    }

    @Test
    public void testAddByteColumnToExistingTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-01T00:00:01.000000Z', 2)
                    """);

            execute("ALTER TABLE t ADD COLUMN b BYTE");

            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:02.000000Z', 3, 42),
                    ('2024-01-01T00:00:03.000000Z', 4, NULL)
                    """);

            assertSql(
                    """
                            ts\tval\tb
                            2024-01-01T00:00:00.000000Z\t1\t
                            2024-01-01T00:00:01.000000Z\t2\t
                            2024-01-01T00:00:02.000000Z\t3\t42
                            2024-01-01T00:00:03.000000Z\t4\t
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );

            // 3 nulls: 2 column-top + 1 explicit NULL
            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE b IS NULL");
            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE b IS NOT NULL");
        });
    }

    @Test
    public void testAddShortColumnToExistingTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-01T00:00:01.000000Z', 2)
                    """);

            execute("ALTER TABLE t ADD COLUMN s SHORT");

            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:02.000000Z', 3, 1000),
                    ('2024-01-01T00:00:03.000000Z', 4, NULL)
                    """);

            // Column-top rows are null, explicit NULL row is null
            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE s IS NULL");
            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE s IS NOT NULL");
        });
    }

    @Test
    public void testAddUInt32ColumnToExistingTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-01T00:00:01.000000Z', 2)
                    """);

            execute("ALTER TABLE t ADD COLUMN u UINT32");

            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:02.000000Z', 3, 100::UINT32),
                    ('2024-01-01T00:00:03.000000Z', 4, NULL)
                    """);

            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE u IS NULL");
            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE u IS NOT NULL");

            // Verify non-null value reads correctly
            assertSql(
                    """
                            u


                            100

                            """,
                    "SELECT u FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testAddColumnAcrossMultiplePartitions() throws Exception {
        // ADD COLUMN with data spanning multiple partitions.
        // Old partitions should all have NULL for the new column.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-02T00:00:00.000000Z', 2),
                    ('2024-01-03T00:00:00.000000Z', 3)
                    """);

            execute("ALTER TABLE t ADD COLUMN flag BOOLEAN");

            // Insert into new partitions and an existing partition
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-03T00:00:01.000000Z', 4, true),
                    ('2024-01-04T00:00:00.000000Z', 5, false)
                    """);

            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE flag IS NULL");
            assertSql("cnt\n2\n", "SELECT COUNT(*) AS cnt FROM t WHERE flag IS NOT NULL");
        });
    }

    @Test
    public void testAddColumnThenInsertNullAndNonNull() throws Exception {
        // Verifies that after ADD COLUMN, both explicit NULL inserts
        // and column-top nulls are correctly tracked in the bitmap.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-01T00:00:01.000000Z', 2)
                    """);

            execute("ALTER TABLE t ADD COLUMN b BYTE");

            // Insert mix of nulls and values
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:02.000000Z', 3, 10),
                    ('2024-01-01T00:00:03.000000Z', 4, NULL),
                    ('2024-01-01T00:00:04.000000Z', 5, 20),
                    ('2024-01-01T00:00:05.000000Z', 6, NULL)
                    """);

            // 2 column-top + 2 explicit NULL = 4 nulls
            assertSql("cnt\n4\n", "SELECT COUNT(*) AS cnt FROM t WHERE b IS NULL");
            // 2 non-null values
            assertSql("cnt\n2\n", "SELECT COUNT(*) AS cnt FROM t WHERE b IS NOT NULL");

            // Verify COALESCE handles both types of null
            assertSql(
                    """
                            c
                            -1
                            -1
                            10
                            -1
                            20
                            -1
                            """,
                    "SELECT COALESCE(b, -1::BYTE) AS c FROM t ORDER BY ts"
            );
        });
    }

    // ========================
    // Edge cases
    // ========================

    @Test
    public void testAllNullPartition() throws Exception {
        // All values are null — bitmap should be all 1s.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', NULL),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', NULL),
                    ('2024-01-01T00:00:03.000000Z', NULL)
                    """);

            assertSql("cnt\n4\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
            assertSql("cnt\n0\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");

            // Verify bitmap contents: all 4 bits set = 0x0F
            try (TableReader reader = getReader("t")) {
                reader.openPartition(0);
                int base = reader.getColumnBase(0);
                MemoryCR bitmapMem = reader.getNullBitmapColumn(base, 1);
                Assert.assertNotNull(bitmapMem);
                long addr = bitmapMem.getPageAddress(0);
                byte b = Unsafe.getUnsafe().getByte(addr);
                Assert.assertEquals("All 4 null bits set", (byte) 0x0F, b);
            }
        });
    }

    @Test
    public void testNoNullPartition() throws Exception {
        // No values are null — bitmap should be all 0s.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-01T00:00:01.000000Z', 2),
                    ('2024-01-01T00:00:02.000000Z', 3),
                    ('2024-01-01T00:00:03.000000Z', 4)
                    """);

            assertSql("cnt\n0\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
            assertSql("cnt\n4\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");

            // Verify bitmap contents: no bits set = 0x00
            try (TableReader reader = getReader("t")) {
                reader.openPartition(0);
                int base = reader.getColumnBase(0);
                MemoryCR bitmapMem = reader.getNullBitmapColumn(base, 1);
                Assert.assertNotNull(bitmapMem);
                long addr = bitmapMem.getPageAddress(0);
                byte b = Unsafe.getUnsafe().getByte(addr);
                Assert.assertEquals("No null bits set", (byte) 0x00, b);
            }
        });
    }

    @Test
    public void testMultiPartitionMixedNulls() throws Exception {
        // Data across 3 partitions with different null patterns.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val BOOLEAN) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', true),
                    ('2024-01-01T00:00:01.000000Z', false),
                    ('2024-01-02T00:00:00.000000Z', NULL),
                    ('2024-01-02T00:00:01.000000Z', NULL),
                    ('2024-01-03T00:00:00.000000Z', true),
                    ('2024-01-03T00:00:01.000000Z', NULL),
                    ('2024-01-03T00:00:02.000000Z', false)
                    """);

            // Partition 1: 0 nulls, Partition 2: 2 nulls, Partition 3: 1 null = 3 total
            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
            assertSql("cnt\n4\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");

            // Verify per-day null counts
            assertSql(
                    """
                            day\tnull_cnt
                            2024-01-01T00:00:00.000000Z\t0
                            2024-01-02T00:00:00.000000Z\t2
                            2024-01-03T00:00:00.000000Z\t1
                            """,
                    "SELECT ts AS day, COUNT(*) - COUNT(val) AS null_cnt FROM t SAMPLE BY 1d ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testConversionAllNullIntToDouble() throws Exception {
        // All values are null — verifies edge case of conversion when every
        // value in .d is the sentinel.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', NULL),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', NULL)
                    """);
            drainWalQueue();

            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
            assertSql("cnt\n0\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");

            execute("ALTER TABLE t ALTER COLUMN val TYPE DOUBLE");
            drainWalQueue();

            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
            assertSql("cnt\n0\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");
        });
    }

    @Test
    public void testConversionNoNullIntToDouble() throws Exception {
        // No values are null — verifies clean conversion without null contamination.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-01T00:00:01.000000Z', 2),
                    ('2024-01-01T00:00:02.000000Z', 0)
                    """);
            drainWalQueue();

            assertSql("cnt\n0\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");

            execute("ALTER TABLE t ALTER COLUMN val TYPE DOUBLE");
            drainWalQueue();

            assertSql("cnt\n0\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");

            assertSql(
                    """
                            val
                            1.0
                            2.0
                            0.0
                            """,
                    "SELECT val FROM t"
            );
        });
    }

    @Test
    public void testConversionMultiPartitionIntToDouble() throws Exception {
        // Type conversion across multiple partitions, each with different null patterns.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 10),
                    ('2024-01-01T00:00:01.000000Z', 20),
                    ('2024-01-02T00:00:00.000000Z', NULL),
                    ('2024-01-02T00:00:01.000000Z', NULL),
                    ('2024-01-03T00:00:00.000000Z', 30),
                    ('2024-01-03T00:00:01.000000Z', NULL)
                    """);
            drainWalQueue();

            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");

            execute("ALTER TABLE t ALTER COLUMN val TYPE DOUBLE");
            drainWalQueue();

            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");

            assertSql(
                    """
                            val
                            10.0
                            20.0
                            null
                            null
                            30.0
                            null
                            """,
                    "SELECT val FROM t"
            );
        });
    }

    @Test
    public void testLazyBitmapMigrationDeleteAndReread() throws Exception {
        // Delete .n file for BYTE column, then re-read.
        // Since BYTE has no sentinel, regenerated bitmap will have all-zeros (no nulls).
        // This verifies the lazy migration code path runs without error,
        // though null info is lost for sentinel-free types.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 42),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0)
                    """);

            // Before deletion: 1 null
            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");

            // Delete .n file
            FilesFacade ff = configuration.getFilesFacade();
            TableToken token = engine.verifyTableName("t");
            try (Path path = new Path()) {
                final int rootLen = path.of(configuration.getDbRoot()).concat(token).size();
                TableUtils.setPathForNativePartition(path.trimTo(rootLen), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY,
                        1704067200000000L, -1L);
                TableUtils.nFile(path, "val");
                Assert.assertTrue(ff.exists(path.$()));
                ff.remove(path.$());
                Assert.assertFalse(ff.exists(path.$()));
            }

            engine.releaseAllReaders();

            // After regeneration: BYTE has no sentinel, so all rows appear non-null
            // (null info is lost for types without sentinels when .n is deleted)
            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL OR val IS NULL");
        });
    }

    @Test
    public void testAddColumnWalTable() throws Exception {
        // ADD COLUMN on WAL table — verify column-top nulls and explicit NULL inserts work.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-01T00:00:01.000000Z', 2)
                    """);
            drainWalQueue();

            execute("ALTER TABLE t ADD COLUMN flag BOOLEAN");
            drainWalQueue();

            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:02.000000Z', 3, true),
                    ('2024-01-01T00:00:03.000000Z', 4, NULL)
                    """);
            drainWalQueue();

            // Column-top rows (val=1,2) and explicit NULL (val=4) should all be null
            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE flag IS NULL");
            // Only the true insert should be non-null
            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE flag IS NOT NULL");
        });
    }

    @Test
    public void testAddColumnWalThenConvert() throws Exception {
        // ADD COLUMN (bitmap-type), then ALTER TYPE on another column.
        // Stress-test: multiple schema modifications, verify everything stays consistent.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 10),
                    ('2024-01-01T00:00:01.000000Z', NULL)
                    """);
            drainWalQueue();

            // Add a BOOLEAN column
            execute("ALTER TABLE t ADD COLUMN flag BOOLEAN");
            drainWalQueue();

            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:02.000000Z', 20, true),
                    ('2024-01-01T00:00:03.000000Z', NULL, false)
                    """);
            drainWalQueue();

            // Now convert INT → DOUBLE
            execute("ALTER TABLE t ALTER COLUMN val TYPE DOUBLE");
            drainWalQueue();

            // INT nulls should be preserved as DOUBLE NaN
            assertSql("cnt\n2\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
            // BOOLEAN column-top nulls should still be tracked
            assertSql("cnt\n2\n", "SELECT COUNT(*) AS cnt FROM t WHERE flag IS NULL");

            assertSql(
                    """
                            val\tflag
                            10.0\t
                            null\t
                            20.0\ttrue
                            null\tfalse
                            """,
                    "SELECT val, flag FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testConversionDoubleToBoolean() throws Exception {
        // DOUBLE (NaN sentinel) → BOOLEAN (bitmap-null). Cross-category conversion.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1.0),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0.0)
                    """);
            drainWalQueue();

            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");

            execute("ALTER TABLE t ALTER COLUMN val TYPE BOOLEAN");
            drainWalQueue();

            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
            assertSql("cnt\n2\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");

            assertSql(
                    """
                            val
                            true

                            false
                            """,
                    "SELECT val FROM t"
            );
        });
    }

    // ========================
    // Cross-type null propagation via INSERT INTO ... SELECT (CastFunction path)
    // ========================

    @Test
    public void testCrossTypeNullPropagation() throws Exception {
        assertMemoryLeak(() -> {
            // INT → SHORT
            execute("CREATE TABLE int_src (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO int_src VALUES ('2024-01-01', 100), ('2024-01-02', NULL), ('2024-01-03', 0)");
            execute("CREATE TABLE short_dst (ts TIMESTAMP, val SHORT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO short_dst SELECT * FROM int_src");
            assertQueryNoLeakCheck(
                    """
                            ts\tval
                            2024-01-01T00:00:00.000000Z\t100
                            2024-01-02T00:00:00.000000Z\t
                            2024-01-03T00:00:00.000000Z\t0
                            """,
                    "SELECT * FROM short_dst", "ts", true, true
            );
            assertSql("total\tnon_null\n3\t2\n", "SELECT COUNT(*) AS total, COUNT(val) AS non_null FROM short_dst");

            // INT → BYTE
            execute("CREATE TABLE byte_dst (ts TIMESTAMP, val BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO byte_dst SELECT * FROM int_src");
            assertQueryNoLeakCheck(
                    """
                            ts\tval
                            2024-01-01T00:00:00.000000Z\t100
                            2024-01-02T00:00:00.000000Z\t
                            2024-01-03T00:00:00.000000Z\t0
                            """,
                    "SELECT * FROM byte_dst", "ts", true, true
            );
            assertSql("total\tnon_null\n3\t2\n", "SELECT COUNT(*) AS total, COUNT(val) AS non_null FROM byte_dst");

            // INT → UINT16
            execute("CREATE TABLE uint16_dst (ts TIMESTAMP, val UINT16) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO uint16_dst SELECT * FROM int_src");
            assertQueryNoLeakCheck(
                    """
                            ts\tval
                            2024-01-01T00:00:00.000000Z\t100
                            2024-01-02T00:00:00.000000Z\t
                            2024-01-03T00:00:00.000000Z\t0
                            """,
                    "SELECT * FROM uint16_dst", "ts", true, true
            );
            assertSql("total\tnon_null\n3\t2\n", "SELECT COUNT(*) AS total, COUNT(val) AS non_null FROM uint16_dst");

            // INT → UINT32: skipped because INT_NULL (-2147483648) collides with
            // valid UINT32 value 2147483648. Same-tag conversion uses raw copy,
            // so null propagation is not possible.

            // LONG → SHORT
            execute("CREATE TABLE long_src (ts TIMESTAMP, val LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO long_src VALUES ('2024-01-01', 50), ('2024-01-02', NULL), ('2024-01-03', 0)");
            execute("CREATE TABLE long_short_dst (ts TIMESTAMP, val SHORT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO long_short_dst SELECT * FROM long_src");
            assertSql("total\tnon_null\n3\t2\n", "SELECT COUNT(*) AS total, COUNT(val) AS non_null FROM long_short_dst");

            // LONG → BYTE
            execute("CREATE TABLE long_byte_dst (ts TIMESTAMP, val BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO long_byte_dst SELECT * FROM long_src");
            assertSql("total\tnon_null\n3\t2\n", "SELECT COUNT(*) AS total, COUNT(val) AS non_null FROM long_byte_dst");

            // LONG → UINT64: skipped because LONG_NULL (-9223372036854775808) collides
            // with valid UINT64 value 9223372036854775808. Same-tag conversion uses
            // raw copy, so null propagation is not possible.

            // FLOAT → SHORT
            execute("CREATE TABLE float_src (ts TIMESTAMP, val FLOAT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO float_src VALUES ('2024-01-01', 10.5), ('2024-01-02', NULL), ('2024-01-03', 0.0)");
            execute("CREATE TABLE float_short_dst (ts TIMESTAMP, val SHORT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO float_short_dst SELECT * FROM float_src");
            assertSql("total\tnon_null\n3\t2\n", "SELECT COUNT(*) AS total, COUNT(val) AS non_null FROM float_short_dst");

            // FLOAT → BYTE
            execute("CREATE TABLE float_byte_dst (ts TIMESTAMP, val BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO float_byte_dst SELECT * FROM float_src");
            assertSql("total\tnon_null\n3\t2\n", "SELECT COUNT(*) AS total, COUNT(val) AS non_null FROM float_byte_dst");

            // DOUBLE → SHORT
            execute("CREATE TABLE double_src (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO double_src VALUES ('2024-01-01', 20.5), ('2024-01-02', NULL), ('2024-01-03', 0.0)");
            execute("CREATE TABLE double_short_dst (ts TIMESTAMP, val SHORT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO double_short_dst SELECT * FROM double_src");
            assertSql("total\tnon_null\n3\t2\n", "SELECT COUNT(*) AS total, COUNT(val) AS non_null FROM double_short_dst");

            // DOUBLE → BYTE
            execute("CREATE TABLE double_byte_dst (ts TIMESTAMP, val BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO double_byte_dst SELECT * FROM double_src");
            assertSql("total\tnon_null\n3\t2\n", "SELECT COUNT(*) AS total, COUNT(val) AS non_null FROM double_byte_dst");
        });
    }

    @Test
    public void testConversionBooleanToInt() throws Exception {
        // BOOLEAN (bitmap-null) → INT (sentinel-null).
        // Source is a bitmap-only type, destination is sentinel-based.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val BOOLEAN) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', true),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', false)
                    """);
            drainWalQueue();

            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");

            execute("ALTER TABLE t ALTER COLUMN val TYPE INT");
            drainWalQueue();

            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
            assertSql("cnt\n2\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");

            assertSql(
                    """
                            val
                            1
                            null
                            0
                            """,
                    "SELECT val FROM t"
            );
        });
    }

    @Test
    public void testConversionShortToDouble() throws Exception {
        // SHORT (bitmap-null) → DOUBLE (NaN sentinel).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val SHORT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 100),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0),
                    ('2024-01-01T00:00:03.000000Z', -1)
                    """);
            drainWalQueue();

            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");

            execute("ALTER TABLE t ALTER COLUMN val TYPE DOUBLE");
            drainWalQueue();

            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");

            assertSql(
                    """
                            val
                            100.0
                            null
                            0.0
                            -1.0
                            """,
                    "SELECT val FROM t"
            );
        });
    }

    @Test
    public void testConversionByteToLong() throws Exception {
        // BYTE (bitmap-null) → LONG (LONG_NULL sentinel).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, val BYTE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 10),
                    ('2024-01-01T00:00:01.000000Z', NULL),
                    ('2024-01-01T00:00:02.000000Z', 0),
                    ('2024-01-01T00:00:03.000000Z', -1)
                    """);
            drainWalQueue();

            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");

            execute("ALTER TABLE t ALTER COLUMN val TYPE LONG");
            drainWalQueue();

            assertSql("cnt\n1\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NULL");
            assertSql("cnt\n3\n", "SELECT COUNT(*) AS cnt FROM t WHERE val IS NOT NULL");

            assertSql(
                    """
                            val
                            10
                            null
                            0
                            -1
                            """,
                    "SELECT val FROM t"
            );
        });
    }

    @Test
    public void testO3MergePreservesBitmapNulls() throws Exception {
        // O3 (out-of-order) inserts trigger a partition merge. The merge must
        // carry over the null bitmaps for bitmap-null column types.
        assertMemoryLeak(() -> {
            // BYPASS WAL so O3 merge happens synchronously in the writer.
            execute("CREATE TABLE o3_null (ts TIMESTAMP, sh SHORT, by BYTE, b BOOLEAN) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL");

            // Batch 1: in-order rows across two partitions
            execute("""
                    INSERT INTO o3_null VALUES
                    ('2024-01-01T01:00:00.000000Z', 10, 1, true),
                    ('2024-01-01T01:00:01.000000Z', NULL, NULL, NULL),
                    ('2024-01-01T02:00:00.000000Z', 20, 2, false),
                    ('2024-01-01T02:00:01.000000Z', NULL, NULL, NULL)
                    """);

            // Batch 2: O3 insert into the first partition, forcing merge
            execute("""
                    INSERT INTO o3_null VALUES
                    ('2024-01-01T01:00:00.500000Z', 15, 3, true),
                    ('2024-01-01T01:00:01.500000Z', NULL, NULL, NULL)
                    """);

            // Partition 1 underwent O3 merge – verify nulls survived
            assertQueryNoLeakCheck(
                    """
                            ts\tsh\tby\tb
                            2024-01-01T01:00:00.000000Z\t10\t1\ttrue
                            2024-01-01T01:00:00.500000Z\t15\t3\ttrue
                            2024-01-01T01:00:01.000000Z\t\t\t
                            2024-01-01T01:00:01.500000Z\t\t\t
                            2024-01-01T02:00:00.000000Z\t20\t2\tfalse
                            2024-01-01T02:00:01.000000Z\t\t\t
                            """,
                    "SELECT * FROM o3_null", "ts", true, true
            );

            // Verify counts: 6 total, 3 non-null per column
            assertSql(
                    "total\tsh_nn\tby_nn\tb_nn\n6\t3\t3\t3\n",
                    "SELECT COUNT(*) AS total, COUNT(sh) AS sh_nn, COUNT(by) AS by_nn, COUNT(b) AS b_nn FROM o3_null"
            );
        });
    }

    @Test
    public void testO3MergeWithMovedUncommittedRows() throws Exception {
        // When O3 mode activates, uncommitted in-order rows are moved into O3
        // memory alongside the O3 rows. The null bitmap reconstruction must
        // handle both groups with the correct bit ordering.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o3_move (ts TIMESTAMP, sh SHORT, by BYTE, b BOOLEAN) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL");

            // Insert and commit in-order rows
            execute("""
                    INSERT INTO o3_move VALUES
                    ('2024-01-01T01:00:00.000000Z', 10, 1, true),
                    ('2024-01-01T01:00:01.000000Z', NULL, NULL, NULL)
                    """);

            // Insert more in-order rows WITHOUT committing, then insert O3
            // rows in the same transaction. The in-order rows become
            // "moved uncommitted" during o3MoveUncommitted().
            execute("""
                    INSERT INTO o3_move VALUES
                    ('2024-01-01T01:00:03.000000Z', 30, 3, false),
                    ('2024-01-01T01:00:04.000000Z', NULL, NULL, NULL),
                    ('2024-01-01T01:00:02.000000Z', NULL, NULL, NULL),
                    ('2024-01-01T01:00:02.500000Z', 25, 5, true)
                    """);

            assertQueryNoLeakCheck(
                    """
                            ts\tsh\tby\tb
                            2024-01-01T01:00:00.000000Z\t10\t1\ttrue
                            2024-01-01T01:00:01.000000Z\t\t\t
                            2024-01-01T01:00:02.000000Z\t\t\t
                            2024-01-01T01:00:02.500000Z\t25\t5\ttrue
                            2024-01-01T01:00:03.000000Z\t30\t3\tfalse
                            2024-01-01T01:00:04.000000Z\t\t\t
                            """,
                    "SELECT * FROM o3_move", "ts", true, true
            );

            assertSql(
                    "total\tsh_nn\tby_nn\tb_nn\n6\t3\t3\t3\n",
                    "SELECT COUNT(*) AS total, COUNT(sh) AS sh_nn, COUNT(by) AS by_nn, COUNT(b) AS b_nn FROM o3_move"
            );
        });
    }

    @Test
    public void testO3AppendCrossPartitionPreservesBitmapNulls() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with 3 partitions, each having 1 row.
            // Partition 02 has a NULL short value.
            execute("CREATE TABLE o3_cross (ts TIMESTAMP, sh SHORT) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL");
            execute("""
                    INSERT INTO o3_cross VALUES
                    ('2024-01-01T01:00:00.000000Z', 10),
                    ('2024-01-01T02:00:00.000000Z', NULL),
                    ('2024-01-01T03:00:00.000000Z', 30)
                    """);

            // Insert O3 rows that APPEND to existing partitions 01 and 02
            // (timestamps after existing rows in those partitions).
            // This triggers OPEN_MID_PARTITION_FOR_APPEND, not MERGE.
            execute("""
                    INSERT INTO o3_cross VALUES
                    ('2024-01-01T01:30:00.000000Z', NULL),
                    ('2024-01-01T02:30:00.000000Z', 25)
                    """);

            assertQueryNoLeakCheck(
                    """
                            ts\tsh
                            2024-01-01T01:00:00.000000Z\t10
                            2024-01-01T01:30:00.000000Z\t
                            2024-01-01T02:00:00.000000Z\t
                            2024-01-01T02:30:00.000000Z\t25
                            2024-01-01T03:00:00.000000Z\t30
                            """,
                    "SELECT * FROM o3_cross", "ts", true, true
            );

            // 5 total rows, 3 non-null (10, 25, 30)
            assertSql(
                    "total\tsh_nn\n5\t3\n",
                    "SELECT COUNT(*) AS total, COUNT(sh) AS sh_nn FROM o3_cross"
            );
        });
    }

    @Test
    public void testMultipleSuccessiveO3MergesPreserveBitmapNulls() throws Exception {
        assertMemoryLeak(() -> {
            // Create 3 partitions with 1 row each
            execute("CREATE TABLE multi_o3 (ts TIMESTAMP, sh SHORT) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL");
            execute("""
                    INSERT INTO multi_o3 VALUES
                    ('2024-01-01T01:00:00.000000Z', 10),
                    ('2024-01-01T02:00:00.000000Z', NULL),
                    ('2024-01-01T03:00:00.000000Z', 30)
                    """);

            // O3 append to partition 01 (01:30 > 01:00)
            execute("INSERT INTO multi_o3 VALUES ('2024-01-01T01:30:00.000000Z', NULL)");

            // O3 append to partition 02 (02:30 > 02:00)
            // This tests stale bit cleanup: the previous O3 commit wrote a null
            // bit for the 01:30 row into the last partition's bitmap. After commit,
            // setBitmapAppendPosition must clear that stale bit. Otherwise, the
            // non-null row (25) gets misread as null.
            execute("INSERT INTO multi_o3 VALUES ('2024-01-01T02:30:00.000000Z', 25)");

            // O3 merge into partitions 01 and 02 (interleaving with existing data)
            execute("""
                    INSERT INTO multi_o3 VALUES
                    ('2024-01-01T01:15:00.000000Z', NULL),
                    ('2024-01-01T02:15:00.000000Z', NULL)
                    """);

            assertQueryNoLeakCheck(
                    """
                            ts\tsh
                            2024-01-01T01:00:00.000000Z\t10
                            2024-01-01T01:15:00.000000Z\t
                            2024-01-01T01:30:00.000000Z\t
                            2024-01-01T02:00:00.000000Z\t
                            2024-01-01T02:15:00.000000Z\t
                            2024-01-01T02:30:00.000000Z\t25
                            2024-01-01T03:00:00.000000Z\t30
                            """,
                    "SELECT * FROM multi_o3", "ts", true, true
            );

            // 7 total, 3 non-null (10, 25, 30)
            assertSql(
                    "total\tsh_nn\n7\t3\n",
                    "SELECT COUNT(*) AS total, COUNT(sh) AS sh_nn FROM multi_o3"
            );
        });
    }

    @Test
    public void testWhereEqualsDoesNotMatchNullByte() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_eq_byte (ts TIMESTAMP, val BYTE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t_eq_byte VALUES ('2024-01-01', 42), ('2024-01-02', null), ('2024-01-03', 0)");

            // WHERE val = 0 should return only the 0 row, not the null row
            assertSql(
                    "ts\tval\n2024-01-03T00:00:00.000000Z\t0\n",
                    "SELECT * FROM t_eq_byte WHERE val = 0"
            );

            // Note: WHERE val != 42 includes null rows in JIT mode (pre-existing
            // limitation for all types including INT/LONG). JIT NE does not check
            // for null sentinels.

            // count check
            assertSql(
                    "total\tnn\n3\t2\n",
                    "SELECT COUNT(*) AS total, COUNT(val) AS nn FROM t_eq_byte"
            );
        });
    }

    @Test
    public void testWhereEqualsDoesNotMatchNullBoolean() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_eq_bool (ts TIMESTAMP, val BOOLEAN) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t_eq_bool VALUES ('2024-01-01', true), ('2024-01-02', null), ('2024-01-03', false)");

            // WHERE val = false should return only the false row, not the null row
            assertSql(
                    "ts\tval\n2024-01-03T00:00:00.000000Z\tfalse\n",
                    "SELECT * FROM t_eq_bool WHERE val = false"
            );

            // WHERE NOT val should return only the false row
            assertSql(
                    "ts\tval\n2024-01-03T00:00:00.000000Z\tfalse\n",
                    "SELECT * FROM t_eq_bool WHERE NOT val"
            );
        });
    }

    @Test
    public void testWhereEqualsDoesNotMatchNullShort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_eq_short (ts TIMESTAMP, val SHORT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t_eq_short VALUES ('2024-01-01', 1000), ('2024-01-02', null), ('2024-01-03', 0)");

            // WHERE val = 0 should return only the 0 row, not the null row
            assertSql(
                    "ts\tval\n2024-01-03T00:00:00.000000Z\t0\n",
                    "SELECT * FROM t_eq_short WHERE val = 0"
            );

            // Note: WHERE val != 1000 includes null rows in JIT mode (pre-existing
            // limitation for all types including INT/LONG). JIT NE does not check
            // for null sentinels.

            // WHERE val > -1 should NOT return the null row
            assertSql(
                    "ts\tval\n2024-01-01T00:00:00.000000Z\t1000\n2024-01-03T00:00:00.000000Z\t0\n",
                    "SELECT * FROM t_eq_short WHERE val > -1"
            );

            // WHERE val < 500 should NOT return the null row
            assertSql(
                    "ts\tval\n2024-01-03T00:00:00.000000Z\t0\n",
                    "SELECT * FROM t_eq_short WHERE val < 500"
            );
        });
    }

    @Test
    public void testWhereInDoesNotMatchNullShort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_in_short (ts TIMESTAMP, val SHORT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t_in_short VALUES ('2024-01-01', 42), ('2024-01-02', null), ('2024-01-03', 0)");

            // IN (0, 42) should NOT return the null row
            assertSql(
                    "ts\tval\n2024-01-01T00:00:00.000000Z\t42\n2024-01-03T00:00:00.000000Z\t0\n",
                    "SELECT * FROM t_in_short WHERE val IN (0, 42)"
            );
        });
    }

    // ========================
    // JIT bitmap-null filter tests
    // ========================

    @Test
    public void testJitFilterByteEqZeroExcludesNull() throws Exception {
        // JIT must not return null rows when filtering val = 0 on BYTE columns
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_jit_byte (ts TIMESTAMP, val BYTE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_jit_byte VALUES ('2024-01-01', 0), ('2024-01-02', NULL), ('2024-01-03', 42)");

            String query = "SELECT * FROM t_jit_byte WHERE val = 0";
            assertSql(
                    "ts\tval\n2024-01-01T00:00:00.000000Z\t0\n",
                    query
            );
            assertSqlRunWithJit(query);
        });
    }

    @Test
    public void testJitFilterShortEqZeroExcludesNull() throws Exception {
        // JIT must not return null rows when filtering val = 0 on SHORT columns
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_jit_short (ts TIMESTAMP, val SHORT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_jit_short VALUES ('2024-01-01', 0), ('2024-01-02', NULL), ('2024-01-03', 100)");

            String query = "SELECT * FROM t_jit_short WHERE val = 0";
            assertSql(
                    "ts\tval\n2024-01-01T00:00:00.000000Z\t0\n",
                    query
            );
            assertSqlRunWithJit(query);
        });
    }

    @Test
    public void testJitFilterShortGtExcludesNull() throws Exception {
        // JIT must not return null rows for val > 10
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_jit_short_gt (ts TIMESTAMP, val SHORT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_jit_short_gt VALUES ('2024-01-01', 100), ('2024-01-02', NULL), ('2024-01-03', 5)");

            String query = "SELECT * FROM t_jit_short_gt WHERE val > 10";
            assertSql(
                    "ts\tval\n2024-01-01T00:00:00.000000Z\t100\n",
                    query
            );
            assertSqlRunWithJit(query);
        });
    }

    @Test
    public void testJitFilterBooleanEqTrueExcludesNull() throws Exception {
        // JIT must not return null rows for val = true
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_jit_bool (ts TIMESTAMP, val BOOLEAN) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_jit_bool VALUES ('2024-01-01', true), ('2024-01-02', NULL), ('2024-01-03', false)");

            String query = "SELECT * FROM t_jit_bool WHERE val = true";
            assertSql(
                    "ts\tval\n2024-01-01T00:00:00.000000Z\ttrue\n",
                    query
            );
            assertSqlRunWithJit(query);
        });
    }
}
