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
import io.questdb.std.FilesFacade;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
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
}
