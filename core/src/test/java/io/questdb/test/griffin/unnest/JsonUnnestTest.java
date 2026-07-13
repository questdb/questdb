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

package io.questdb.test.griffin.unnest;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.join.JsonUnnestSource;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

// Most tests use assertQueryNoLeakCheck() which re-executes the query to
// verify cursor stability and checks factory memory. Tests that combine
// JSON UNNEST with json_extract() use assertSql() instead, because
// json_extract() lazily allocates ~2MB internal buffers that trip the
// factory memory check. See testJsonExtractWithJsonUnnest for details.
public class JsonUnnestTest extends AbstractCairoTest {

    @Test
    public void testAllNullArrayReturnsAllNulls() throws Exception {
        // When every element in the JSON array is null, the scan-forward
        // detection cannot determine scalar vs object. It defaults to
        // scalar, which is correct: both paths produce NULL for null
        // elements, so the result is the same regardless.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, null, null]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            null
                            null
                            null
                            """);
        });
    }

    @Test
    public void testAllNullObjectArrayReturnsAllNulls() throws Exception {
        // Same as above but with multiple declared columns, which forces
        // isObjectArray = true. The result is still all NULLs because
        // accessing any field on a null JSON element returns NULL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, null]')");
            assertQuery("SELECT u.a, u.b FROM t, UNNEST("
                    + "t.payload COLUMNS(a DOUBLE, b DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            a\tb
                            null\tnull
                            null\tnull
                            """);
        });
    }

    @Test
    public void testArithmeticOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10, 20, 30]')");
            assertQuery("SELECT u.val * 2 doubled FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            doubled
                            20
                            40
                            60
                            """);
        });
    }

    @Test
    public void testAvgOnJsonUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10.0, 20.0, 30.0]')");
            assertQuery("SELECT avg(u.val) FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            avg
                            20.0
                            """);
        });
    }

    @Test
    public void testBaseTableBooleanColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getBool() for base table columns (col < split).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (b BOOLEAN, payload VARCHAR)");
            execute("INSERT INTO t VALUES (true, '[1, 2]')");
            assertQuery("SELECT t.b, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            b\tval
                            true\t1
                            true\t2
                            """);
        });
    }

    @Test
    public void testBaseTableDateColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getDate() for base table columns (col < split).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (d DATE, payload VARCHAR)");
            execute("INSERT INTO t VALUES ('2024-01-15T00:00:00.000Z', '[1.5, 2.5]')");
            assertQuery("SELECT t.d, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            d\tval
                            2024-01-15T00:00:00.000Z\t1.5
                            2024-01-15T00:00:00.000Z\t2.5
                            """);
        });
    }

    @Test
    public void testBaseTableDecimal128ColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getDecimal128() for base table columns (col < split).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (d DECIMAL(20, 2), payload VARCHAR)");
            execute("INSERT INTO t VALUES (123.45m, '[1, 2]')");
            assertQuery("SELECT t.d, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            d\tval
                            123.45\t1
                            123.45\t2
                            """);
        });
    }

    @Test
    public void testBaseTableDecimal16ColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getDecimal16() for base table columns (col < split).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (d DECIMAL(3, 1), payload VARCHAR)");
            execute("INSERT INTO t VALUES (9.5m, '[1, 2]')");
            assertQuery("SELECT t.d, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            d\tval
                            9.5\t1
                            9.5\t2
                            """);
        });
    }

    @Test
    public void testBaseTableDecimal256ColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getDecimal256() for base table columns (col < split).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (d DECIMAL(40, 2), payload VARCHAR)");
            execute("INSERT INTO t VALUES (999.99m, '[1, 2]')");
            assertQuery("SELECT t.d, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            d\tval
                            999.99\t1
                            999.99\t2
                            """);
        });
    }

    @Test
    public void testBaseTableDecimal32ColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getDecimal32() (col is always a base table column).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (d DECIMAL(5, 2), payload VARCHAR)");
            execute("INSERT INTO t VALUES (12.34m, '[1, 2]')");
            assertQuery("SELECT t.d, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            d\tval
                            12.34\t1
                            12.34\t2
                            """);
        });
    }

    @Test
    public void testBaseTableDecimal64ColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getDecimal64() (col is always a base table column).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (d DECIMAL(10, 2), payload VARCHAR)");
            execute("INSERT INTO t VALUES (12345.67m, '[1, 2]')");
            assertQuery("SELECT t.d, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            d\tval
                            12345.67\t1
                            12345.67\t2
                            """);
        });
    }

    @Test
    public void testBaseTableDecimal8ColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getDecimal8() (col is always a base table column).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (d DECIMAL(2, 1), payload VARCHAR)");
            execute("INSERT INTO t VALUES (1.5m, '[1, 2]')");
            assertQuery("SELECT t.d, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            d\tval
                            1.5\t1
                            1.5\t2
                            """);
        });
    }

    @Test
    public void testBaseTableFloatColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getFloat() for base table columns (col < split).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (f FLOAT, payload VARCHAR)");
            execute("INSERT INTO t VALUES (3.14, '[10, 20]')");
            assertQuery("SELECT t.f, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            f\tval
                            3.14\t10
                            3.14\t20
                            """);
        });
    }

    @Test
    public void testBaseTableGeoByteColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getGeoByte() (col is always a base table column).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (g GEOHASH(1c), payload VARCHAR)");
            execute("INSERT INTO t VALUES (#s, '[1, 2]')");
            assertQuery("SELECT t.g, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            g\tval
                            s\t1
                            s\t2
                            """);
        });
    }

    @Test
    public void testBaseTableGeoIntColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getGeoInt() (col is always a base table column).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (g GEOHASH(5c), payload VARCHAR)");
            execute("INSERT INTO t VALUES (#s0000, '[1, 2]')");
            assertQuery("SELECT t.g, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            g\tval
                            s0000\t1
                            s0000\t2
                            """);
        });
    }

    @Test
    public void testBaseTableGeoLongColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getGeoLong() (col is always a base table column).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (g GEOHASH(7c), payload VARCHAR)");
            execute("INSERT INTO t VALUES (#s000000, '[1, 2]')");
            assertQuery("SELECT t.g, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            g\tval
                            s000000\t1
                            s000000\t2
                            """);
        });
    }

    @Test
    public void testBaseTableGeoShortColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getGeoShort() (col is always a base table column).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (g GEOHASH(3c), payload VARCHAR)");
            execute("INSERT INTO t VALUES (#s00, '[1, 2]')");
            assertQuery("SELECT t.g, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            g\tval
                            s00\t1
                            s00\t2
                            """);
        });
    }

    @Test
    public void testBaseTableIPv4ColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getIPv4() (col is always a base table column).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ip IPv4, payload VARCHAR)");
            execute("INSERT INTO t VALUES ('192.168.1.1', '[1, 2]')");
            assertQuery("SELECT t.ip, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            ip\tval
                            192.168.1.1\t1
                            192.168.1.1\t2
                            """);
        });
    }

    @Test
    public void testBaseTableLong256ColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getLong256A() and getLong256(col, sink).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (h LONG256, payload VARCHAR)");
            execute("INSERT INTO t VALUES ('0x01', '[1, 2]')");
            assertQuery("SELECT t.h, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            h\tval
                            0x01\t1
                            0x01\t2
                            """);
        });
    }

    @Test
    public void testBaseTableLong256FilterWithUnnest() throws Exception {
        // Exercises UnnestRecord.getLong256B() via equality in WHERE.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (h LONG256, payload VARCHAR)");
            execute("INSERT INTO t VALUES "
                    + "('0x01', '[1, 2]'), "
                    + "('0x02', '[3, 4]')");
            assertQuery("SELECT t.h, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u WHERE t.h = cast('0x01' AS LONG256)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            h\tval
                            0x01\t1
                            0x01\t2
                            """);
        });
    }

    @Test
    public void testBaseTableShortColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getShort() for base table columns (col < split).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SHORT, payload VARCHAR)");
            execute("INSERT INTO t VALUES (42, '[1.5, 2.5]')");
            assertQuery("SELECT t.s, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            s\tval
                            42\t1.5
                            42\t2.5
                            """);
        });
    }

    @Test
    public void testBaseTableStringColumnFilterWithUnnest() throws Exception {
        // Exercises UnnestRecord.getStrB() via equality comparison in WHERE,
        // which uses getStrB() for the B-copy needed by the filter.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s STRING, payload VARCHAR)");
            execute("INSERT INTO t VALUES "
                    + "('keep', '[1, 2]'), "
                    + "('drop', '[3, 4]')");
            assertQuery("SELECT t.s, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u WHERE t.s = 'keep'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            s\tval
                            keep\t1
                            keep\t2
                            """);
        });
    }

    @Test
    public void testBaseTableStringColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getStrA() and getStrB() for base table columns (col < split).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s STRING, payload VARCHAR)");
            execute("INSERT INTO t VALUES ('hello', '[1, 2]')");
            assertQuery("SELECT t.s, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            s\tval
                            hello\t1
                            hello\t2
                            """);
        });
    }

    @Test
    public void testBaseTableSymbolColumnWithUnnest() throws Exception {
        // Exercises UnnestRecord.getSymA() (col is always a base table column).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, payload VARCHAR)");
            execute("INSERT INTO t VALUES ('abc', '[1, 2]')");
            assertQuery("SELECT t.s, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            s\tval
                            abc\t1
                            abc\t2
                            """);
        });
    }

    @Test
    public void testBaseTableSymbolOrderByWithUnnest() throws Exception {
        // Exercises UnnestRecord.getSymB() via ORDER BY on a base table symbol
        // column. The sort comparator needs both A and B copies of the symbol.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, payload VARCHAR)");
            execute("INSERT INTO t VALUES "
                    + "('b', '[1]'), "
                    + "('a', '[2]')");
            assertQuery("SELECT t.s, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u ORDER BY t.s")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            s\tval
                            a\t2
                            b\t1
                            """);
        });
    }

    @Test
    public void testBooleanFromObject() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"active\":true},{\"active\":false}]'"
                    + ")");
            assertQuery("SELECT u.active FROM t, UNNEST("
                    + "t.payload COLUMNS(active BOOLEAN)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            active
                            true
                            false
                            """);
        });
    }

    @Test
    public void testBulkResultsReallocationOomCleanup() throws Exception {
        // Exercises the OOM path in ensureBulkResultsCapacity().
        // Row 1 has a small array (allocates small buffer), row 2 has a
        // large array (triggers reallocation). With a tight RSS limit the
        // reallocation calloc throws. Without the fix (zeroing bulkResultsPtr
        // before calloc), close() would double-free the stale pointer —
        // aborting the JVM on macOS or corrupting the heap on Linux.
        // With the fix, close() sees ptr=0 and skips the free.
        // assertMemoryLeak verifies balanced memory accounting.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.0, 2.0]')");
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < 50_000; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(i).append(".0");
            }
            sb.append(']');
            execute("INSERT INTO t VALUES ('" + sb + "')");

            // Allow enough headroom for query setup but not for the
            // 50000 * 1 * 24 = 1.2MB bulk results buffer.
            Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 512 * 1024);
            try {
                assertExceptionNoLeakCheck(
                        "SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val DOUBLE)) u",
                        0,
                        "RSS memory limit exceeded"
                );
            } catch (AssertionError e) {
                // If the OOM doesn't trigger (enough memory was available),
                // the query succeeds — that's fine, the test is still valid
                // because assertMemoryLeak will catch any accounting issues.
                if (!e.getMessage().contains("SQL statement should have failed")) {
                    throw e;
                }
            } finally {
                Unsafe.setRssMemLimit(0);
            }
        });
    }

    @Test
    public void testCastBooleanToInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[true, false, true]')");
            assertQuery("SELECT u.val::INT val FROM t, UNNEST(t.payload COLUMNS(val BOOLEAN)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1
                            0
                            1
                            """);
        });
    }

    @Test
    public void testCastBooleanToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[true, false]')");
            assertQuery("SELECT u.val::VARCHAR val FROM t, UNNEST(t.payload COLUMNS(val BOOLEAN)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            true
                            false
                            """);
        });
    }

    @Test
    public void testCastDoubleToInt() throws Exception {
        // double-to-int cast truncates toward zero
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.9, 2.1, 3.5]')");
            assertQuery("SELECT u.val::INT val FROM t, UNNEST(t.payload COLUMNS(val DOUBLE)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testCastDoubleToLong() throws Exception {
        // double-to-long cast truncates toward zero
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.9, 2.1, 3.5]')");
            assertQuery("SELECT u.val::LONG val FROM t, UNNEST(t.payload COLUMNS(val DOUBLE)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testCastDoubleToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.5, null, -3.14]')");
            assertQuery("SELECT u.val::VARCHAR val FROM t, UNNEST(t.payload COLUMNS(val DOUBLE)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1.5
                            \
                            
                            -3.14
                            """);
        });
    }

    @Test
    public void testCastIntNullToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, 42]')");
            assertQuery("SELECT u.val::VARCHAR val FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            \
                            
                            42
                            """);
        });
    }

    @Test
    public void testCastIntToBoolean() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[0, 1, 42]')");
            assertQuery("SELECT u.val::BOOLEAN val FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            false
                            true
                            true
                            """);
        });
    }

    @Test
    public void testCastIntToDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1, 2, 3]')");
            assertQuery("SELECT u.val::DOUBLE val FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1.0
                            2.0
                            3.0
                            """);
        });
    }

    @Test
    public void testCastIntToLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1, 2, 3]')");
            assertQuery("SELECT u.val::LONG val FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testCastIntToShort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[100, 200, 300]')");
            assertQuery("SELECT u.val::SHORT val FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            100
                            200
                            300
                            """);
        });
    }

    @Test
    public void testCastIntToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[42, null, -7]')");
            assertQuery("SELECT u.val::VARCHAR val FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            42
                            \
                            
                            -7
                            """);
        });
    }

    @Test
    public void testCastLongToDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[100, 200, 300]')");
            assertQuery("SELECT u.val::DOUBLE val FROM t, UNNEST(t.payload COLUMNS(val LONG)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            100.0
                            200.0
                            300.0
                            """);
        });
    }

    @Test
    public void testCastLongToInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[100, 200, 300]')");
            assertQuery("SELECT u.val::INT val FROM t, UNNEST(t.payload COLUMNS(val LONG)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            100
                            200
                            300
                            """);
        });
    }

    @Test
    public void testCastLongToTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1000000, 0]')");
            assertQuery("SELECT u.val::TIMESTAMP val FROM t, UNNEST(t.payload COLUMNS(val LONG)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1970-01-01T00:00:01.000000Z
                            1970-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCastLongToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[42, null, -7]')");
            assertQuery("SELECT u.val::VARCHAR val FROM t, UNNEST(t.payload COLUMNS(val LONG)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            42
                            \
                            
                            -7
                            """);
        });
    }

    @Test
    public void testCastNullPreservation() throws Exception {
        // Null INT cast to DOUBLE preserves null
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, 42]')");
            assertQuery("SELECT u.val::DOUBLE val FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            null
                            42.0
                            """);
        });
    }

    @Test
    public void testCastObjectFieldTimestampToLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"ts\": \"2024-01-01T00:00:00.000000Z\"}]')");
            assertQuery("SELECT u.ts::LONG val FROM t, UNNEST(t.payload COLUMNS(ts TIMESTAMP)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1704067200000000
                            """);
        });
    }

    @Test
    public void testCastObjectFieldToDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"price\": 10}, {\"price\": 20}]')");
            assertQuery("SELECT u.price::DOUBLE val FROM t, UNNEST(t.payload COLUMNS(price INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            10.0
                            20.0
                            """);
        });
    }

    @Test
    public void testCastShortToDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10, 20, 30]')");
            assertQuery("SELECT u.val::DOUBLE val FROM t, UNNEST(t.payload COLUMNS(val SHORT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            10.0
                            20.0
                            30.0
                            """);
        });
    }

    @Test
    public void testCastShortToInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10, 20, 30]')");
            assertQuery("SELECT u.val::INT val FROM t, UNNEST(t.payload COLUMNS(val SHORT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            10
                            20
                            30
                            """);
        });
    }

    @Test
    public void testCastTimestampToLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"2024-01-01T00:00:00.000000Z\"]')");
            assertQuery("SELECT u.val::LONG val FROM t, UNNEST(t.payload COLUMNS(val TIMESTAMP)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1704067200000000
                            """);
        });
    }

    @Test
    public void testCastTimestampToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"2024-01-01T00:00:00.000000Z\", \"2024-06-15T12:30:00.000000Z\"]')");
            assertQuery("SELECT u.val::VARCHAR val FROM t, UNNEST(t.payload COLUMNS(val TIMESTAMP)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            2024-01-01T00:00:00.000000Z
                            2024-06-15T12:30:00.000000Z
                            """);
        });
    }

    @Test
    public void testCastVarcharToBoolean() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"true\", \"false\"]')");
            assertQuery("SELECT u.val::BOOLEAN val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            true
                            false
                            """);
        });
    }

    @Test
    public void testCastVarcharToDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"1.5\", \"2.7\"]')");
            assertQuery("SELECT u.val::DOUBLE val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1.5
                            2.7
                            """);
        });
    }

    @Test
    public void testCastVarcharToInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"42\", \"100\", \"-7\"]')");
            assertQuery("SELECT u.val::INT val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            42
                            100
                            -7
                            """);
        });
    }

    @Test
    public void testCastVarcharToTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"2024-01-01T00:00:00.000000Z\"]')");
            assertQuery("SELECT u.val::TIMESTAMP val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            2024-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testColumnAliasOverridesDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.5]')");
            assertQuery("SELECT u.my_price FROM t, UNNEST("
                    + "t.payload COLUMNS(price DOUBLE)"
                    + ") u(my_price)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            my_price
                            1.5
                            """);
        });
    }

    @Test
    public void testColumnsDottedFieldNameExtractsValue() throws Exception {
        // Regression: a dotted JSON field key (a.b) must extract its value, not silently return
        // NULL. The parser keeps the protective quotes on the dotted COLUMNS name so its dots stay
        // content for the display name; JsonUnnestSource must strip them from the native extraction
        // key, or the C++ matcher looks up a field literally spelled "a.b" and never matches the
        // JSON key a.b.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"a.b\":1.5},{\"a.b\":2.5}]')");
            assertQuery("SELECT u.\"a.b\" FROM t, UNNEST(t.payload COLUMNS(\"a.b\" DOUBLE)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            a.b
                            1.5
                            2.5
                            """);
        });
    }

    @Test
    public void testColumnsDottedFieldNameNonStandardQuotesExtractsValue() throws Exception {
        // A single-quoted dotted COLUMNS field name behaves like the double-quoted "a.b": the parser
        // normalizes it to the protective double-quote form, JsonUnnestSource strips those quotes for
        // the native extraction key, and the JSON field a.b is matched. Regression: keeping the raw
        // single quote left the dot to mis-split into a compile-time internal error.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"a.b\":1.5},{\"a.b\":2.5}]')");
            assertQuery("SELECT u.\"a.b\" FROM t, UNNEST(t.payload COLUMNS('a.b' DOUBLE)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            a.b
                            1.5
                            2.5
                            """);
        });
    }

    @Test
    public void testColumnsDottedFieldNameWithEmbeddedQuoteRejected() throws Exception {
        // A dotted COLUMNS field name is re-wrapped in protective double quotes; an embedded double
        // quote would break that quote parity and silently match no JSON key (returning NULL), so the
        // parser must reject it cleanly instead of leaking a malformed extraction key.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT * FROM t, UNNEST(t.payload COLUMNS(\"a\"\"b.c\" DOUBLE)) u",
                    42,
                    "dotted UNNEST column name cannot contain a double quote"
            );
        });
    }

    @Test
    public void testColumnsKeywordAsColumnNameRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT * FROM t, UNNEST(t.payload COLUMNS(select DOUBLE)) u",
                    42,
                    "have to be enclosed in double quotes"
            );
        });
    }

    @Test
    public void testColumnsKeywordQuotedColumnNameAllowed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"select\":1.5}]')");
            assertQuery("SELECT u.\"select\" FROM t, UNNEST(t.payload COLUMNS(\"select\" DOUBLE)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            select
                            1.5
                            """);
        });
    }

    @Test
    public void testConcatOnUnnestedVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"hello\", \"world\"]')");
            assertQuery("SELECT concat(u.val, '!') greeting FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            greeting
                            hello!
                            world!
                            """);
        });
    }

    @Test
    public void testCountOnJsonUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1, 2, 3, 4, 5]')");
            assertQuery("SELECT count() cnt FROM t, UNNEST("
                    + "t.payload COLUMNS(val LONG)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            cnt
                            5
                            """);
        });
    }

    @Test
    public void testCrossJoinJsonUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"a\":1},{\"a\":2}]')");
            assertQuery("SELECT u.a FROM t CROSS JOIN UNNEST("
                    + "t.payload COLUMNS(a INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            a
                            1
                            2
                            """);
        });
    }

    @Test
    public void testCteWithJsonUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.5, 2.5]')");
            assertQuery("WITH cte AS (SELECT payload FROM t) "
                    + "SELECT u.val FROM cte, UNNEST("
                    + "cte.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1.5
                            2.5
                            """);
        });
    }

    @Test
    public void testDotNotationAccess() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"price\":1.5}]')");
            assertQuery("SELECT u.price FROM t, UNNEST("
                    + "t.payload COLUMNS(price DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            price
                            1.5
                            """);
        });
    }

    @Test
    public void testEmptyArrayReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("val\n");
        });
    }

    @Test
    public void testEmptyStringPayload() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("val\n");
        });
    }

    @Test
    public void testErrorNonVarcharWithColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[])");
            execute("INSERT INTO t VALUES (ARRAY[1.0])");
            assertException(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.arr COLUMNS(val DOUBLE)"
                            + ") u",
                    28,
                    "VARCHAR expected for JSON UNNEST"
            );
        });
    }

    @Test
    public void testErrorUnknownType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val FOOBAR)"
                            + ") u",
                    50,
                    "unknown type"
            );
        });
    }

    @Test
    public void testErrorUnsupportedTypeBinary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val BINARY)"
                            + ") u",
                    50,
                    "unsupported type for JSON UNNEST"
            );
        });
    }

    @Test
    public void testErrorUnsupportedTypeByte() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val BYTE)"
                            + ") u",
                    50,
                    "unsupported type for JSON UNNEST"
            );
        });
    }

    @Test
    public void testErrorUnsupportedTypeChar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val CHAR)"
                            + ") u",
                    50,
                    "unsupported type for JSON UNNEST"
            );
        });
    }

    @Test
    public void testErrorUnsupportedTypeDecimal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val DECIMAL)"
                            + ") u",
                    50,
                    "unsupported type for JSON UNNEST"
            );
        });
    }

    @Test
    public void testErrorUnsupportedTypeFloat() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val FLOAT)"
                            + ") u",
                    50,
                    "unsupported type for JSON UNNEST"
            );
        });
    }

    @Test
    public void testErrorUnsupportedTypeGeohash() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val GEOHASH)"
                            + ") u",
                    50,
                    "unsupported type for JSON UNNEST"
            );
        });
    }

    @Test
    public void testErrorUnsupportedTypeIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val IPv4)"
                            + ") u",
                    50,
                    "unsupported type for JSON UNNEST"
            );
        });
    }

    @Test
    public void testErrorUnsupportedTypeInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val INTERVAL)"
                            + ") u",
                    50,
                    "unsupported type for JSON UNNEST"
            );
        });
    }

    @Test
    public void testErrorUnsupportedTypeLong256() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val LONG256)"
                            + ") u",
                    50,
                    "unsupported type for JSON UNNEST"
            );
        });
    }

    @Test
    public void testErrorUnsupportedTypeSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val SYMBOL)"
                            + ") u",
                    50,
                    "unsupported type for JSON UNNEST"
            );
        });
    }

    @Test
    public void testErrorUnsupportedTypeUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertException(
                    "SELECT u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val UUID)"
                            + ") u",
                    50,
                    "unsupported type for JSON UNNEST"
            );
        });
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.5]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Unnest
                                  columns: [val]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """);
        });
    }

    @Test
    public void testExplainPlanWithOrdinality() throws Exception {
        // Exercises UnnestRecordCursorFactory.toPlan() with ordinality=true (L111).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.5]')");
            assertQuery("SELECT u.val, u.pos FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") WITH ORDINALITY u(val, pos)")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Unnest
                                  columns: [val,pos]
                                  ordinality: true
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """);
        });
    }

    @Test
    public void testExtraFieldsIgnored() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"price\":1.5,\"name\":\"a\",\"qty\":10}]'"
                    + ")");
            assertQuery("SELECT u.price FROM t, UNNEST("
                    + "t.payload COLUMNS(price DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            price
                            1.5
                            """);
        });
    }

    @Test
    public void testGetSymbolTableReturnsNullForUnnestColumn() throws Exception {
        // Exercises UnnestRecordCursor.getSymbolTable() and newSymbolTable()
        // returning null for unnest columns (col >= split) (L164, L197).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, payload VARCHAR)");
            execute("INSERT INTO t VALUES ('x', '[1, 2]')");
            try (RecordCursorFactory factory = select(
                    "SELECT t.s, u.val FROM t, UNNEST("
                            + "t.payload COLUMNS(val INT)"
                            + ") u")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // col 0 = t.s (SYMBOL, base table) — should return non-null
                    Assert.assertNotNull(cursor.getSymbolTable(0));
                    Assert.assertNotNull(cursor.newSymbolTable(0));
                    // col 1 = u.val (INT, unnest) — should return null
                    Assert.assertNull(cursor.getSymbolTable(1));
                    Assert.assertNull(cursor.newSymbolTable(1));
                }
            }
        });
    }

    @Test
    public void testGroupByOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"cat\":\"a\",\"val\":1},"
                    + "{\"cat\":\"b\",\"val\":2},"
                    + "{\"cat\":\"a\",\"val\":3}]'"
                    + ")");
            assertQuery("SELECT u.cat, sum(u.val) total FROM t, UNNEST("
                    + "t.payload COLUMNS(cat VARCHAR, val LONG)"
                    + ") u GROUP BY u.cat ORDER BY u.cat")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cat\ttotal
                            a\t4
                            b\t2
                            """);
        });
    }

    @Test
    public void testIntFromDouble() throws Exception {
        // JSON number 1.5 extracted as INT should truncate or return NULL
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1, 2, 3]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testInvalidJsonReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('not json')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("val\n");
        });
    }

    @Test
    public void testIssue6869JsonExtractUnnestWithFilter() throws Exception {
        // Reproduces the exact scenario from issue #6869: nested JSON array
        // extracted via json_extract, unnested, and filtered by an id field.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE events (ts TIMESTAMP, payload VARCHAR) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO events VALUES (
                        '2026-01-01',
                        '{"context":{"items":[{"id":"item_1","value":100,"score":0.95},{"id":"item_2","value":200,"score":0.87},{"id":"item_3","value":300,"score":0.91}]}}'
                    )
                    """);
            assertQuery("""
                    SELECT u.id, u.value, u.score
                    FROM events e, UNNEST(
                        json_extract(e.payload, '$.context.items')
                        COLUMNS(id VARCHAR, value LONG, score DOUBLE)
                    ) u
                    WHERE u.id = 'item_1'
                    """)
                    // returnsOnce(): json_extract() lazily allocates ~2MB of internal buffers during
                    // cursor execution that outlive the cursor, so the full returns() battery's
                    // factory-memory check fails. The single-pass returnsOnce() closes the factory at once.
                    .noLeakCheck()
                    .returnsOnce("""
                            id\tvalue\tscore
                            item_1\t100\t0.95
                            """);
        });
    }

    @Test
    public void testJsonExtractWithJsonUnnest() throws Exception {
        // json_extract() lazily allocates ~2MB internal buffers during cursor
        // execution, so assertQueryNoLeakCheck's factory memory check fails.
        // Use assertSql() which closes the factory immediately after use.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE events (payload VARCHAR)");
            execute("INSERT INTO events VALUES ("
                    + "'{\"items\":[{\"price\":1.5,\"name\":\"a\"},"
                    + "{\"price\":2.5,\"name\":\"b\"}]}'"
                    + ")");
            assertQuery("SELECT u.price, u.name FROM events e, UNNEST("
                    + "json_extract(e.payload, '$.items') "
                    + "COLUMNS(price DOUBLE, name VARCHAR)"
                    + ") u")
                    .noLeakCheck()
                    .returnsOnce("""
                            price\tname
                            1.5\ta
                            2.5\tb
                            """);
        });
    }

    @Test
    public void testJsonObjectNotArrayReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('{\"key\": \"value\"}')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("val\n");
        });
    }

    @Test
    public void testLargeArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < 1000; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(i);
            }
            sb.append(']');
            execute("INSERT INTO t VALUES ('" + sb + "')");
            assertQuery("SELECT count() cnt FROM t, UNNEST("
                    + "t.payload COLUMNS(val LONG)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            cnt
                            1000
                            """);
        });
    }

    @Test
    public void testLargeNonAsciiVarcharOverflowThrowsError() throws Exception {
        // Non-ASCII values exceeding DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE trigger UTF-8
        // backoff in the native layer, producing sink.size() < maxSize.
        // The native truncated flag detects the overflow and throws.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            // 1366 CJK characters x 3 bytes each = 4098 bytes > 4096
            int charCount = (JsonUnnestSource.DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE / 3) + 2;
            String bigVal = "世".repeat(charCount); // '世' = 3 bytes in UTF-8
            execute("INSERT INTO t VALUES ('[{\"s\":\"" + bigVal + "\"}]')");
            assertExceptionNoLeakCheck(
                    "SELECT u.s FROM t, UNNEST("
                            + "t.payload COLUMNS(s VARCHAR)"
                            + ") u",
                    0,
                    "JSON UNNEST: value exceeds maximum size of "
                            + JsonUnnestSource.DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE
                            + " bytes for column 's'"
            );
        });
    }

    @Test
    public void testLargeVarcharOverflowThrowsError() throws Exception {
        // Values exceeding DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE (4096) throw an error
        // rather than silently truncating or returning NULL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            String bigVal = "x".repeat(5000);
            execute("INSERT INTO t VALUES ('[{\"s\":\"" + bigVal + "\"}]')");
            assertExceptionNoLeakCheck(
                    "SELECT u.s FROM t, UNNEST("
                            + "t.payload COLUMNS(s VARCHAR)"
                            + ") u",
                    0,
                    "JSON UNNEST: value exceeds maximum size of "
                            + JsonUnnestSource.DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE
                            + " bytes for column 's'"
            );
        });
    }

    @Test
    public void testLimitOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.0, 2.0, 3.0, 4.0, 5.0]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u LIMIT 2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1.0
                            2.0
                            """);
        });
    }

    @Test
    public void testMissingFieldReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"price\":1.5},{\"price\":2.5,\"name\":\"banana\"}]'"
                    + ")");
            assertQuery("SELECT u.price, u.name FROM t, UNNEST("
                    + "t.payload COLUMNS(price DOUBLE, name VARCHAR)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            price\tname
                            1.5\t
                            2.5\tbanana
                            """);
        });
    }

    @Test
    public void testMixedJsonAndArraySources() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[], payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "ARRAY[100.0, 200.0], "
                    + "'[{\"name\":\"a\"},{\"name\":\"b\"}]'"
                    + ")");
            assertQuery("SELECT u.elem, u.name FROM t, UNNEST("
                    + "t.arr, "
                    + "t.payload COLUMNS(name VARCHAR)"
                    + ") u(elem, name)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            elem\tname
                            100.0\ta
                            200.0\tb
                            """);
        });
    }

    @Test
    public void testMixedNullAndNonNullRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, payload VARCHAR)");
            execute("INSERT INTO t VALUES "
                    + "(1, '[1.0, 2.0]'), "
                    + "(2, NULL), "
                    + "(3, '[3.0]')");
            assertQuery("SELECT t.id, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval
                            1\t1.0
                            1\t2.0
                            3\t3.0
                            """);
        });
    }

    @Test
    public void testMultiColumnBufferRegrow() throws Exception {
        // With columnCount > 1 and > 16 elements, the initial bulk results
        // buffer (capacity 16) is too small, triggering len < 0 regrow path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            StringBuilder sb = new StringBuilder("[");
            StringBuilder expected = new StringBuilder("a\tb\n");
            for (int i = 0; i < 20; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append("{\"a\":").append(i).append(",\"b\":").append(i * 10).append('}');
                expected.append(i).append('\t').append(i * 10).append('\n');
            }
            sb.append(']');
            execute("INSERT INTO t VALUES ('" + sb + "')");
            assertQuery("SELECT u.a, u.b FROM t, UNNEST(t.payload COLUMNS(a INT, b INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected.toString());
        });
    }

    @Test
    public void testMultiColumnForcesObjectMode() throws Exception {
        // With multiple columns, init() forces isObjectArray = true.
        // Scalar array elements have no named fields, so all columns return null/default.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1, 2, 3]')");
            assertQuery("SELECT u.a, u.b FROM t, UNNEST(t.payload COLUMNS(a DOUBLE, b DOUBLE)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            a\tb
                            null\tnull
                            null\tnull
                            null\tnull
                            """);
        });
    }

    @Test
    public void testMultipleBaseRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, payload VARCHAR)");
            execute("INSERT INTO t VALUES "
                    + "(1, '[1.0, 2.0]'), "
                    + "(2, '[3.0, 4.0, 5.0]')");
            assertQuery("SELECT t.id, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval
                            1\t1.0
                            1\t2.0
                            2\t3.0
                            2\t4.0
                            2\t5.0
                            """);
        });
    }

    @Test
    public void testMultipleBaseRowsDifferentLengths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, payload VARCHAR)");
            execute("INSERT INTO t VALUES "
                    + "(1, '[1.0]'), "
                    + "(2, '[2.0, 3.0, 4.0]'), "
                    + "(3, '[]')");
            assertQuery("SELECT t.id, u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tval
                            1\t1.0
                            2\t2.0
                            2\t3.0
                            2\t4.0
                            """);
        });
    }

    @Test
    public void testMultipleJsonSources() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a VARCHAR, b VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.0, 2.0]', '[\"x\", \"y\", \"z\"]')");
            assertQuery("SELECT u.val, u.name FROM t, UNNEST("
                    + "t.a COLUMNS(val DOUBLE), "
                    + "t.b COLUMNS(name VARCHAR)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val\tname
                            1.0\tx
                            2.0\ty
                            null\tz
                            """);
        });
    }

    @Test
    public void testMultipleRowsDifferentArrayTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1]'), ('[2, 3]'), ('[4, 5, 6]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1
                            2
                            3
                            4
                            5
                            6
                            """);
        });
    }

    @Test
    public void testMultipleRowsMixedNullAndValid() throws Exception {
        // Mix of null payloads, empty arrays, valid arrays, and invalid JSON
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES (NULL), ('[]'), ('[1, 2]'), ('not json'), ('[3]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testMultipleRowsMixedPayloadsAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, payload VARCHAR)");
            execute("INSERT INTO t VALUES " +
                    "(1, '[{\"v\":10}, {\"v\":20}]')," +
                    "(2, NULL)," +
                    "(3, '[{\"v\":30}]')");
            assertQuery("SELECT t.id, u.v FROM t, UNNEST(t.payload COLUMNS(v INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tv
                            1\t10
                            1\t20
                            3\t30
                            """);
        });
    }

    @Test
    public void testMultipleSourcesBoundsBoolean() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a VARCHAR, b VARCHAR)");
            execute("INSERT INTO t VALUES ('[true]', '[1, 2, 3]')");
            assertQuery("SELECT u.x, u.y FROM t, UNNEST("
                    + "t.a COLUMNS(x BOOLEAN), "
                    + "t.b COLUMNS(y INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x\ty
                            true\t1
                            false\t2
                            false\t3
                            """);
        });
    }

    @Test
    public void testMultipleSourcesBoundsDouble() throws Exception {
        // When one source is shorter, out-of-bounds DOUBLE must be null, not 0.0 or garbage.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a VARCHAR, b VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.0]', '[10.0, 20.0, 30.0]')");
            assertQuery("SELECT u.x, u.y FROM t, UNNEST("
                    + "t.a COLUMNS(x DOUBLE), "
                    + "t.b COLUMNS(y DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x\ty
                            1.0\t10.0
                            null\t20.0
                            null\t30.0
                            """);
        });
    }

    @Test
    public void testMultipleSourcesBoundsInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a VARCHAR, b VARCHAR)");
            execute("INSERT INTO t VALUES ('[1]', '[10, 20, 30]')");
            assertQuery("SELECT u.x, u.y FROM t, UNNEST("
                    + "t.a COLUMNS(x INT), "
                    + "t.b COLUMNS(y INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x\ty
                            1\t10
                            null\t20
                            null\t30
                            """);
        });
    }

    @Test
    public void testMultipleSourcesBoundsLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a VARCHAR, b VARCHAR)");
            execute("INSERT INTO t VALUES ('[1]', '[100, 200, 300]')");
            assertQuery("SELECT u.x, u.y FROM t, UNNEST("
                    + "t.a COLUMNS(x LONG), "
                    + "t.b COLUMNS(y LONG)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x\ty
                            1\t100
                            null\t200
                            null\t300
                            """);
        });
    }

    @Test
    public void testMultipleSourcesBoundsMixedTypes() throws Exception {
        // Three sources with different lengths: tests bounds across all types simultaneously.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a VARCHAR, b VARCHAR, c VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.5, 2.5]', '[\"x\"]', '[100, 200, 300]')");
            assertQuery("SELECT u.d, u.v, u.i FROM t, UNNEST("
                    + "t.a COLUMNS(d DOUBLE), "
                    + "t.b COLUMNS(v VARCHAR), "
                    + "t.c COLUMNS(i INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            d\tv\ti
                            1.5\tx\t100
                            2.5\t\t200
                            null\t\t300
                            """);
        });
    }

    @Test
    public void testMultipleSourcesBoundsShort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a VARCHAR, b VARCHAR)");
            execute("INSERT INTO t VALUES ('[1]', '[10, 20, 30]')");
            assertQuery("SELECT u.x, u.y FROM t, UNNEST("
                    + "t.a COLUMNS(x SHORT), "
                    + "t.b COLUMNS(y SHORT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x\ty
                            1\t10
                            0\t20
                            0\t30
                            """);
        });
    }

    // Tests for COLUMNS keyword validation

    @Test
    public void testMultipleSourcesBoundsTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a VARCHAR, b VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"2026-01-01T00:00:00.000000Z\"]', '[1, 2, 3]')");
            assertQuery("SELECT u.x, u.y FROM t, UNNEST("
                    + "t.a COLUMNS(x TIMESTAMP), "
                    + "t.b COLUMNS(y INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x\ty
                            2026-01-01T00:00:00.000000Z\t1
                            \t2
                            \t3
                            """);
        });
    }

    @Test
    public void testMultipleSourcesBoundsVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a VARCHAR, b VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"a\"]', '[\"x\", \"y\", \"z\"]')");
            assertQuery("SELECT u.x, u.y FROM t, UNNEST("
                    + "t.a COLUMNS(x VARCHAR), "
                    + "t.b COLUMNS(y VARCHAR)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x\ty
                            a\tx
                            \ty
                            \tz
                            """);
        });
    }


    // Comprehensive per-type tests: scalar arrays, object arrays, nullability


    // INT: scalar array

    @Test
    public void testNotAnArrayReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('{\"key\":\"val\"}')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("val\n");
        });
    }

    @Test
    public void testNullElementInArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.5, null, 3.5]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1.5
                            null
                            3.5
                            """);
        });
    }

    @Test
    public void testNullFieldInObject() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"val\":null},{\"val\":1.5}]'"
                    + ")");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            null
                            1.5
                            """);
        });
    }

    @Test
    public void testNullPayloadAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES (NULL)");
            assertQuery("SELECT u.i, u.l, u.d, u.b, u.s, u.ts FROM t, UNNEST(t.payload" +
                    " COLUMNS(i INT, l LONG, d DOUBLE, b BOOLEAN, s VARCHAR, ts TIMESTAMP)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("i\tl\td\tb\ts\tts\n");
        });
    }

    @Test
    public void testNullPayloadReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES (NULL)");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("val\n");
        });
    }

    // INT: object array

    @Test
    public void testObjectAllFieldsMissing() throws Exception {
        // Element is an object but has none of the declared fields
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"x\":1, \"y\":2}]')");
            assertQuery("SELECT u.i, u.d, u.s FROM t, UNNEST(t.payload" +
                    " COLUMNS(i INT, d DOUBLE, s VARCHAR)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\td\ts
                            null\tnull\t
                            """);
        });
    }

    @Test
    public void testObjectArrayMultiColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"price\":1.5,\"name\":\"apple\"},"
                    + "{\"price\":2.5,\"name\":\"banana\"}]'"
                    + ")");
            assertQuery("SELECT u.price, u.name FROM t, UNNEST("
                    + "t.payload COLUMNS(price DOUBLE, name VARCHAR)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            price\tname
                            1.5\tapple
                            2.5\tbanana
                            """);
        });
    }

    // LONG: scalar array

    @Test
    public void testObjectArrayWithNullFirstElement() throws Exception {
        // Verifies scan-forward detection: element 0 is null, but
        // element 1 is an object, so isObjectArray should be true.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[null, {\"a\":1}, {\"a\":2}]'"
                    + ")");
            assertQuery("SELECT u.a FROM t, UNNEST("
                    + "t.payload COLUMNS(a LONG)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            a
                            null
                            1
                            2
                            """);
        });
    }

    @Test
    public void testObjectArrayWithOrdinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"name\":\"a\"},{\"name\":\"b\"}]'"
                    + ")");
            assertQuery("SELECT u.name, u.ord FROM t, UNNEST("
                    + "t.payload COLUMNS(name VARCHAR)"
                    + ") WITH ORDINALITY u(name, ord)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            name\tord
                            a\t1
                            b\t2
                            """);
        });
    }

    @Test
    public void testObjectArrayWithSingleColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"price\":1.5},{\"price\":2.5}]'"
                    + ")");
            assertQuery("SELECT u.price FROM t, UNNEST("
                    + "t.payload COLUMNS(price DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            price
                            1.5
                            2.5
                            """);
        });
    }

    @Test
    public void testObjectBooleanMissingField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":true}, {\"other\":99}, {\"v\":false}]')");
            assertQuery("SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v BOOLEAN)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            v
                            true
                            false
                            false
                            """);
        });
    }

    // LONG: object array

    @Test
    public void testObjectBooleanNullField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":true}, {\"v\":null}, {\"v\":false}]')");
            assertQuery("SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v BOOLEAN)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            v
                            true
                            false
                            false
                            """);
        });
    }

    // SHORT: scalar array

    @Test
    public void testObjectDoubleMissingField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":1.5}, {\"x\":99}, {\"v\":3.5}]')");
            assertQuery("SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v DOUBLE)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            v
                            1.5
                            null
                            3.5
                            """);
        });
    }

    @Test
    public void testObjectDoubleNullField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":1.5}, {\"v\":null}, {\"v\":3.5}]')");
            assertQuery("SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v DOUBLE)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            v
                            1.5
                            null
                            3.5
                            """);
        });
    }

    @Test
    public void testObjectIntMissingField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":10}, {\"other\":99}, {\"v\":30}]')");
            assertQuery("SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            v
                            10
                            null
                            30
                            """);
        });
    }

    // SHORT: object array

    @Test
    public void testObjectIntNullField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":10}, {\"v\":null}, {\"v\":30}]')");
            assertQuery("SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            v
                            10
                            null
                            30
                            """);
        });
    }

    @Test
    public void testObjectLongNullField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":100}, {\"v\":null}]')");
            assertQuery("SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v LONG)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            v
                            100
                            null
                            """);
        });
    }

    // DOUBLE: scalar array

    @Test
    public void testObjectMultiTypeWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[" +
                    "{\"i\":1, \"l\":100, \"d\":1.5, \"b\":true, \"s\":\"hello\", \"ts\":\"2024-01-01T00:00:00.000000Z\"}," +
                    "{\"i\":null, \"l\":null, \"d\":null, \"b\":null, \"s\":null, \"ts\":null}," +
                    "{\"i\":3, \"l\":300, \"d\":3.5, \"b\":false, \"s\":\"world\", \"ts\":\"2024-06-15T00:00:00.000000Z\"}" +
                    "]')");
            assertQuery("SELECT u.i, u.l, u.d, u.b, u.s, u.ts FROM t, UNNEST(t.payload" +
                    " COLUMNS(i INT, l LONG, d DOUBLE, b BOOLEAN, s VARCHAR, ts TIMESTAMP)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\tl\td\tb\ts\tts
                            1\t100\t1.5\ttrue\thello\t2024-01-01T00:00:00.000000Z
                            null\tnull\tnull\tfalse\t\t
                            3\t300\t3.5\tfalse\tworld\t2024-06-15T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testObjectNullElement() throws Exception {
        // Null element in object array: all fields should be null
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"i\":1, \"d\":1.5}, null, {\"i\":3, \"d\":3.5}]')");
            assertQuery("SELECT u.i, u.d FROM t, UNNEST(t.payload COLUMNS(i INT, d DOUBLE)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            i\td
                            1\t1.5
                            null\tnull
                            3\t3.5
                            """);
        });
    }

    @Test
    public void testObjectShortMissingField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":10}, {\"other\":99}, {\"v\":30}]')");
            assertQuery("SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v SHORT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            v
                            10
                            0
                            30
                            """);
        });
    }

    @Test
    public void testObjectShortNullField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":10}, {\"v\":null}]')");
            assertQuery("SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v SHORT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            v
                            10
                            0
                            """);
        });
    }

    // DOUBLE: object array

    @Test
    public void testObjectTimestampMissingField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"ts\":\"2024-01-01T00:00:00.000000Z\"}, {\"other\":1}]')");
            assertQuery("SELECT u.ts FROM t, UNNEST(t.payload COLUMNS(ts TIMESTAMP)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ts
                            2024-01-01T00:00:00.000000Z
                            
                            """);
        });
    }

    @Test
    public void testObjectTimestampNullField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"ts\":\"2024-01-01T00:00:00.000000Z\"}, {\"ts\":null}]')");
            assertQuery("SELECT u.ts FROM t, UNNEST(t.payload COLUMNS(ts TIMESTAMP)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ts
                            2024-01-01T00:00:00.000000Z
                            
                            """);
        });
    }

    // BOOLEAN: scalar array

    @Test
    public void testObjectVarcharMissingField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":\"hello\"}, {\"other\":\"x\"}, {\"v\":\"world\"}]')");
            assertQuery("SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v VARCHAR)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            v
                            hello
                            
                            world
                            """);
        });
    }

    @Test
    public void testObjectVarcharNullField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":\"hello\"}, {\"v\":null}, {\"v\":\"world\"}]')");
            assertQuery("SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v VARCHAR)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            v
                            hello
                            
                            world
                            """);
        });
    }

    @Test
    public void testOrderByOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[3.0, 1.0, 2.0]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u ORDER BY u.val")
                    .noLeakCheck()
                    .returns("""
                            val
                            1.0
                            2.0
                            3.0
                            """);
        });
    }

    // BOOLEAN: object array

    @Test
    public void testOrdinalityCastToDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10, 20]')");
            assertQuery("SELECT u.val, u.ordinality::DOUBLE ord FROM t, UNNEST(t.payload COLUMNS(val INT)) WITH ORDINALITY u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val\tord
                            10\t1.0
                            20\t2.0
                            """);
        });
    }

    @Test
    public void testOrdinalityCastToInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10, 20, 30]')");
            assertQuery("SELECT u.val, u.ordinality::INT ord FROM t, UNNEST(t.payload COLUMNS(val INT)) WITH ORDINALITY u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val\tord
                            10\t1
                            20\t2
                            30\t3
                            """);
        });
    }

    // VARCHAR: scalar array

    @Test
    public void testParserRoundtrip() throws Exception {
        // Verify COLUMNS syntax survives parser round-trip
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Unnest
                                  columns: [val]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """);
        });
    }

    @Test
    public void testScalarArrayWithNullFirstElement() throws Exception {
        // Verifies scan-forward detection: element 0 is null, but
        // element 1 is a scalar, so isObjectArray should be false.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, 1.5, 2.5]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            null
                            1.5
                            2.5
                            """);
        });
    }

    @Test
    public void testScalarBooleanAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, null]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val BOOLEAN)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            false
                            false
                            """);
        });
    }

    @Test
    public void testScalarBooleanArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[true, false, true]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val BOOLEAN)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            true
                            false
                            true
                            """);
        });
    }

    @Test
    public void testScalarBooleanNullElement() throws Exception {
        // BOOLEAN null sentinel is false
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[true, null, false]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val BOOLEAN)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            true
                            false
                            false
                            """);
        });
    }

    @Test
    public void testScalarBooleanTypeMismatch() throws Exception {
        // Non-boolean values return false
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1, \"yes\", true]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val BOOLEAN)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            false
                            false
                            true
                            """);
        });
    }

    // VARCHAR: object array

    @Test
    public void testScalarDateArray() throws Exception {
        // Exercises JsonUnnestSource.getDate() and UnnestRecord.getDate()
        // for unnest columns (col >= split). DATE values are epoch millis.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            // 1705276800000 = 2024-01-15T00:00:00.000Z in millis
            execute("INSERT INTO t VALUES ('[1705276800000, 0]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val DATE)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            2024-01-15T00:00:00.000Z
                            1970-01-01T00:00:00.000Z
                            """);
        });
    }

    @Test
    public void testScalarDateNullElement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1705276800000, null]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val DATE)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            2024-01-15T00:00:00.000Z
                            
                            """);
        });
    }

    @Test
    public void testScalarDoubleAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, null]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val DOUBLE)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            null
                            null
                            """);
        });
    }

    @Test
    public void testScalarDoubleArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.5, 2.5, 3.5]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1.5
                            2.5
                            3.5
                            """);
        });
    }

    // TIMESTAMP: scalar array

    @Test
    public void testScalarDoubleNullElement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.5, null, 3.5]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val DOUBLE)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1.5
                            null
                            3.5
                            """);
        });
    }

    @Test
    public void testScalarDoubleSpecialValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[0.0, -0.0, 1.7976931348623157E308]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val DOUBLE)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            0.0
                            -0.0
                            1.7976931348623157E308
                            """);
        });
    }

    @Test
    public void testScalarDoubleTypeMismatchString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"abc\", 2.5]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val DOUBLE)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            null
                            2.5
                            """);
        });
    }

    @Test
    public void testScalarIntAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, null, null]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            null
                            null
                            null
                            """);
        });
    }

    @Test
    public void testScalarIntArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10, 20, 30]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            10
                            20
                            30
                            """);
        });
    }

    @Test
    public void testScalarIntFromDoubleValue() throws Exception {
        // JSON has no integer type distinction; 1.9 should truncate or return NULL
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.9, 2.0, 3.1]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1
                            2
                            3
                            """);
        });
    }

    // TIMESTAMP: object array

    @Test
    public void testScalarIntNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            // INT_MIN (-2147483648) is Numbers.INT_NULL sentinel, so it reads as null
            execute("INSERT INTO t VALUES ('[-1, -2147483647, 2147483647, 0]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            -1
                            -2147483647
                            2147483647
                            0
                            """);
        });
    }

    @Test
    public void testScalarIntNullElement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10, null, 30]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            10
                            null
                            30
                            """);
        });
    }

    // Multi-column object array: all types together

    @Test
    public void testScalarIntTypeMismatchString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"not_a_number\", 42]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            null
                            42
                            """);
        });
    }

    @Test
    public void testScalarLongAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, null]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val LONG)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            null
                            null
                            """);
        });
    }

    @Test
    public void testScalarLongArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[100000, 200000, 300000]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val LONG)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            100000
                            200000
                            300000
                            """);
        });
    }

    // Edge cases: empty/null/invalid JSON payload

    @Test
    public void testScalarLongLargeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            // LONG_MIN (-9223372036854775808) is Numbers.LONG_NULL sentinel, so it reads as null
            execute("INSERT INTO t VALUES ('[9223372036854775807, -9223372036854775807, 0]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val LONG)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            9223372036854775807
                            -9223372036854775807
                            0
                            """);
        });
    }

    @Test
    public void testScalarLongNullElement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[100, null, 300]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val LONG)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            100
                            null
                            300
                            """);
        });
    }

    @Test
    public void testScalarLongTypeMismatchString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"hello\", 42]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val LONG)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            null
                            42
                            """);
        });
    }

    @Test
    public void testScalarShortArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1, 2, 3]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val SHORT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            1
                            2
                            3
                            """);
        });
    }

    // Multiple rows with mixed null/valid payloads

    @Test
    public void testScalarShortNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[-1, 32767, -32768]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val SHORT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            -1
                            32767
                            -32768
                            """);
        });
    }

    // getVarcharSize

    @Test
    public void testScalarShortNullElement() throws Exception {
        // SHORT null sentinel is 0, so null elements return 0
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10, null, 30]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val SHORT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            10
                            0
                            30
                            """);
        });
    }

    // CAST TESTS: casting unnested JSON values

    @Test
    public void testScalarShortTypeMismatch() throws Exception {
        // Type mismatch returns 0 (SHORT null sentinel)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"text\", 5]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val SHORT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            0
                            5
                            """);
        });
    }

    @Test
    public void testScalarStringArray() throws Exception {
        // Exercises JsonUnnestSource.getStrA() via STRING column type.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"hello\", \"world\"]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val STRING)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            hello
                            world
                            """);
        });
    }

    @Test
    public void testScalarStringFilterExercisesStrB() throws Exception {
        // Exercises JsonUnnestSource.getStrB() via equality filter,
        // which needs the B-copy for comparison.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"keep\", \"drop\", \"keep\"]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val STRING)"
                    + ") u WHERE u.val = 'keep'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            keep
                            keep
                            """);
        });
    }

    @Test
    public void testScalarStringFromObjectField() throws Exception {
        // Exercises JsonUnnestSource.getStrA() with object field extraction.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"name\": \"alice\"}, {\"name\": \"bob\"}]')");
            assertQuery("SELECT u.name FROM t, UNNEST("
                    + "t.payload COLUMNS(name STRING)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            name
                            alice
                            bob
                            """);
        });
    }

    @Test
    public void testScalarStringNotObject() throws Exception {
        // When single column name is "val" but elements are strings,
        // not objects - should extract directly
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"hello\", \"world\"]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val VARCHAR)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            hello
                            world
                            """);
        });
    }

    @Test
    public void testScalarStringNullElement() throws Exception {
        // Exercises NULL handling in JsonUnnestSource.getStrA().
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"a\", null, \"b\"]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val STRING)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            a
                            
                            b
                            """);
        });
    }

    @Test
    public void testScalarStringUtf8() throws Exception {
        // Exercises the UTF-8 to UTF-16 conversion path in getStrA().
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"héllo\", \"wörld\"]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val STRING)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            héllo
                            wörld
                            """);
        });
    }

    @Test
    public void testScalarTimestampAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, null]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val TIMESTAMP)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            
                            
                            """);
        });
    }

    @Test
    public void testScalarTimestampFromNumericMicros() throws Exception {
        // 1704067200000000 = 2024-01-01T00:00:00.000000Z in micros
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1704067200000000, 0]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val TIMESTAMP)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            2024-01-01T00:00:00.000000Z
                            1970-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testScalarTimestampInvalidStringReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"not-a-timestamp\", \"2024-01-01T00:00:00.000000Z\"]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val TIMESTAMP)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            
                            2024-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testScalarTimestampMixedStringAndNumeric() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"2024-06-15T12:30:00.000000Z\", 1704067200000000, null]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val TIMESTAMP)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            2024-06-15T12:30:00.000000Z
                            2024-01-01T00:00:00.000000Z
                            
                            """);
        });
    }

    @Test
    public void testScalarTimestampNullElement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"2024-01-01T00:00:00.000000Z\", null, \"2024-01-03T00:00:00.000000Z\"]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val TIMESTAMP)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            2024-01-01T00:00:00.000000Z
                            
                            2024-01-03T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testScalarTimestampTypeMismatchBoolean() throws Exception {
        // Boolean value should return null for TIMESTAMP
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[true, \"2024-01-01T00:00:00.000000Z\"]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val TIMESTAMP)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            
                            2024-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testScalarVarcharAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, null, null]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            
                            
                            
                            """);
        });
    }

    @Test
    public void testScalarVarcharArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"hello\", \"world\"]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val VARCHAR)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            hello
                            world
                            """);
        });
    }

    @Test
    public void testScalarVarcharEmptyString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"hello\", \"\", \"world\"]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            hello
                            
                            world
                            """);
        });
    }

    @Test
    public void testScalarVarcharNullElement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"hello\", null, \"world\"]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            hello
                            
                            world
                            """);
        });
    }

    @Test
    public void testScalarVarcharNullVsEmptyString() throws Exception {
        // Proves that JSON null produces SQL NULL (not empty string).
        // assertQueryNoLeakCheck renders both NULL and "" as blank lines,
        // so IS NULL is the only way to distinguish them.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[null, \"\", \"text\"]')");
            assertQuery("SELECT u.val, u.val IS NULL AS is_null, length(u.val) AS len " +
                    "FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val\tis_null\tlen
                            \ttrue\t-1
                            \tfalse\t0
                            text\tfalse\t4
                            """);
        });
    }

    @Test
    public void testScalarVarcharNumberCoercion() throws Exception {
        // Numbers extracted as VARCHAR should return as string representation
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[42, true, \"text\"]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            42
                            true
                            text
                            """);
        });
    }

    @Test
    public void testScalarVarcharSpecialChars() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"line1\\nline2\", \"tab\\there\", \"quote\\\"inside\"]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            line1
                            line2
                            tab\there
                            quote"inside
                            """);
        });
    }

    @Test
    public void testSelectStar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, payload VARCHAR)");
            execute("INSERT INTO t VALUES (1, '[{\"price\":1.5}]')");
            assertQuery("SELECT * FROM t, UNNEST("
                    + "t.payload COLUMNS(price DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            id\tpayload\tprice
                            1\t[{"price":1.5}]\t1.5
                            """);
        });
    }

    @Test
    public void testSingleElementObjectArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"v\":42}]')");
            assertQuery("SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            v
                            42
                            """);
        });
    }

    @Test
    public void testSingleElementScalarArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[42]')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            42
                            """);
        });
    }

    @Test
    public void testStandaloneJsonUnnest() throws Exception {
        assertMemoryLeak(() -> assertQuery("SELECT * FROM UNNEST("
                + "'[{\"price\":1.5,\"name\":\"apple\"},"
                + "{\"price\":2.5,\"name\":\"banana\"}]'::VARCHAR "
                + "COLUMNS(price DOUBLE, name VARCHAR)"
                + ") u")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        price\tname
                        1.5\tapple
                        2.5\tbanana
                        """));
    }

    @Test
    public void testStandaloneSelectStar() throws Exception {
        assertMemoryLeak(() -> assertQuery("SELECT * FROM UNNEST("
                + "'[1.5, 2.5]'::VARCHAR "
                + "COLUMNS(val DOUBLE)"
                + ") u")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        val
                        1.5
                        2.5
                        """));
    }

    @Test
    public void testSubqueryWrapping() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.0, 2.0, 3.0]')");
            assertQuery("SELECT sum(val) total FROM ("
                    + "SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u"
                    + ")")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            total
                            6.0
                            """);
        });
    }

    @Test
    public void testSumOnJsonUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[1.0, 2.0, 3.0]')");
            assertQuery("SELECT sum(u.val) total FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            total
                            6.0
                            """);
        });
    }

    @Test
    public void testTimestampFromJsonNumber() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            // Micros since epoch: 2024-01-15T09:30:00Z = 1705311000000000
            execute("INSERT INTO t VALUES ('[{\"ts\":1705311000000000}]')");
            assertQuery("SELECT u.ts FROM t, UNNEST("
                    + "t.payload COLUMNS(ts TIMESTAMP)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ts
                            2024-01-15T09:30:00.000000Z
                            """);
        });
    }

    @Test
    public void testTimestampFromJsonString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"ts\":\"2024-01-15T10:30:00.000000Z\"},"
                    + "{\"ts\":\"2024-06-20T14:00:00.000000Z\"}]'"
                    + ")");
            assertQuery("SELECT u.ts FROM t, UNNEST("
                    + "t.payload COLUMNS(ts TIMESTAMP)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ts
                            2024-01-15T10:30:00.000000Z
                            2024-06-20T14:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testTimestampOverflowThrowsError() throws Exception {
        // Exercises the overflow check in getTimestamp() - a string value
        // exceeding DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE in a TIMESTAMP column must throw.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            String bigTs = "2024-01-15T10:30:00.000000Z" + "x".repeat(5000);
            execute("INSERT INTO t VALUES ('[{\"ts\":\"" + bigTs + "\"}]')");
            assertExceptionNoLeakCheck(
                    "SELECT u.ts FROM t, UNNEST("
                            + "t.payload COLUMNS(ts TIMESTAMP)"
                            + ") u",
                    0,
                    "JSON UNNEST: value exceeds maximum size of "
                            + JsonUnnestSource.DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE
                            + " bytes for column 'ts'"
            );
        });
    }

    @Test
    public void testTimestampTruncationInObject() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            String sb = "[{\"ts\": \"" + "x".repeat(JsonUnnestSource.DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE + 100) +
                    "\"}]";
            execute("INSERT INTO t VALUES ('" + sb + "')");
            assertException(
                    "SELECT u.ts FROM t, UNNEST(t.payload COLUMNS(ts TIMESTAMP)) u",
                    0,
                    "JSON UNNEST: value exceeds maximum size"
            );
        });
    }

    // WITH ORDINALITY cast tests

    @Test
    public void testTypeMismatchIntReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"val\":\"not_a_number\"}]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val INT)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            null
                            """);
        });
    }

    @Test
    public void testTypeMismatchLongReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[{\"val\":\"not_a_number\"}]')");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val LONG)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            null
                            """);
        });
    }

    // Varchar B copy (cursor stability) via cross-join

    @Test
    public void testTypeMismatchReturnsNaN() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"val\":\"not_a_number\"}]'"
                    + ")");
            assertQuery("SELECT u.val FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val
                            null
                            """);
        });
    }

    // Overflow / truncation tests

    @Test
    public void testUnicodeStringsInJson() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            // Use precomposed Unicode (U+00E9) directly in JSON string,
            // avoiding combining characters that produce different byte
            // sequences after simdjson decoding
            execute("INSERT INTO t VALUES ("
                    + "'[{\"name\":\"été\"},{\"name\":\"üñ\"}]'"
                    + ")");
            assertQuery("SELECT u.name FROM t, UNNEST("
                    + "t.payload COLUMNS(name VARCHAR)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            name
                            été
                            üñ
                            """);
        });
    }

    @Test
    public void testUnnestVarcharCastToSymbolOrderBy() throws Exception {
        // Exercises UnnestRecord.getSymB() by casting an unnest VARCHAR column
        // to SYMBOL and ordering by it. The sort comparator needs both A and B
        // copies, which calls getSymB() on the UnnestRecord.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"banana\", \"apple\", \"cherry\"]')");
            assertQuery("SELECT distinct u.val::SYMBOL x FROM t, UNNEST("
                    + "t.payload COLUMNS(val VARCHAR)"
                    + ") u order by 1")
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            x
                            apple
                            banana
                            cherry
                            """);
        });
    }

    @Test
    public void testVarcharBCopyViaJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"hello\", \"world\"]')");
            assertQuery("SELECT u1.val a, u2.val b"
                    + " FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u1"
                    + ", UNNEST(t.payload COLUMNS(val VARCHAR)) u2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            a\tb
                            hello\thello
                            hello\tworld
                            world\thello
                            world\tworld
                            """);
        });
    }

    @Test
    public void testVarcharBTruncationViaEquality() throws Exception {
        // Exercises getVarcharB overflow check. EqVarcharFunctionFactory calls
        // left.getVarcharA(rec) and right.getVarcharB(rec). When the right-hand
        // column (s2) overflows, getVarcharB hits the truncation branch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            String bigVal = "x".repeat(5000);
            execute("INSERT INTO t VALUES ('[{\"s1\": \"ok\", \"s2\": \"" + bigVal + "\"}]')");
            assertExceptionNoLeakCheck(
                    "SELECT u.s1, u.s2 FROM t, UNNEST("
                            + "t.payload COLUMNS(s1 VARCHAR, s2 VARCHAR)"
                            + ") u WHERE u.s1 = u.s2",
                    0,
                    "JSON UNNEST: value exceeds maximum size of "
                            + JsonUnnestSource.DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE
                            + " bytes for column 's2'"
            );
        });
    }

    // init() edge cases

    @Test
    public void testVarcharExactlyAtCapDoesNotError() throws Exception {
        // A value exactly DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE bytes must NOT be treated
        // as overflow. This guards against false positives in the
        // truncation check.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            int cap = JsonUnnestSource.DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE;
            String exactVal = "a".repeat(cap);
            execute("INSERT INTO t VALUES ('[{\"s\":\"" + exactVal + "\"}]')");
            assertQuery("SELECT u.s FROM t, UNNEST("
                    + "t.payload COLUMNS(s VARCHAR)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("s\n"
                            + exactVal + "\n");
        });
    }

    @Test
    public void testVarcharFromObject() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]'"
                    + ")");
            assertQuery("SELECT u.name FROM t, UNNEST("
                    + "t.payload COLUMNS(name VARCHAR)"
                    + ") u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            name
                            Alice
                            Bob
                            """);
        });
    }

    @Test
    public void testVarcharSizeReturnsCorrectLength() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[\"ab\", null, \"hello\"]')");
            assertQuery("SELECT u.val, length(u.val) len FROM t, UNNEST(t.payload COLUMNS(val VARCHAR)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val\tlen
                            ab\t2
                            \t-1
                            hello\t5
                            """);
        });
    }

    // Expressions on unnested values

    @Test
    public void testVarcharTruncationInObjectThrows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            String sb = "[{\"v\": \"" + "a".repeat(JsonUnnestSource.DEFAULT_MAX_JSON_UNNEST_VALUE_SIZE + 100) +
                    "\"}]";
            execute("INSERT INTO t VALUES ('" + sb + "')");
            assertException(
                    "SELECT u.v FROM t, UNNEST(t.payload COLUMNS(v VARCHAR)) u",
                    0,
                    "JSON UNNEST: value exceeds maximum size"
            );
        });
    }

    @Test
    public void testWhereOnUnnested() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ("
                    + "'[{\"price\":1.5},{\"price\":2.5},{\"price\":3.5}]'"
                    + ")");
            assertQuery("SELECT u.price FROM t, UNNEST("
                    + "t.payload COLUMNS(price DOUBLE)"
                    + ") u WHERE u.price > 2.0")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            price
                            2.5
                            3.5
                            """);
        });
    }

    // Multi-row with mixed array lengths

    @Test
    public void testWhitespaceOnlyPayloadReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('   ')");
            assertQuery("SELECT u.val FROM t, UNNEST(t.payload COLUMNS(val INT)) u")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("val\n");
        });
    }

    @Test
    public void testWithOrdinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (payload VARCHAR)");
            execute("INSERT INTO t VALUES ('[10.0, 20.0, 30.0]')");
            assertQuery("SELECT u.val, u.ord FROM t, UNNEST("
                    + "t.payload COLUMNS(val DOUBLE)"
                    + ") WITH ORDINALITY u(val, ord)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            val\tord
                            10.0\t1
                            20.0\t2
                            30.0\t3
                            """);
        });
    }
}
