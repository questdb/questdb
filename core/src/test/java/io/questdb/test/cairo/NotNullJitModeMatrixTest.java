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

package io.questdb.test.cairo;

import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The JIT-mode matrix gate for NOT NULL columns: for every sentinel-bearing type reachable
 * with a stored sentinel, the same predicate must return the same rows under the interpreted
 * filter (JIT_MODE_DISABLED), the scalar compiled filter (JIT_MODE_FORCE_SCALAR) and the AVX2
 * compiled filter (JIT_MODE_ENABLED):
 * <ul>
 * <li>{@code v IS NULL} on a NOT NULL column is 0 and {@code v IS NOT NULL} is the row count;</li>
 * <li>a same-width comparison sees the sentinel bit pattern as DATA;</li>
 * <li>a widened comparison agrees with the same-width one (pins the C10 getter fixes);</li>
 * <li>a nullable control column keeps excluding genuine NULLs everywhere.</li>
 * </ul>
 * Sentinels are stored either via their literal (INT/LONG/DOUBLE/FLOAT/IPv4) or via the
 * reclassification route (CREATE nullable, INSERT NULL, ALTER COLUMN SET NOT NULL), which is
 * a pure metadata flip. Each test pins the JIT mode per query through
 * {@link #assertAcrossJitModes(String, String)}; AbstractCairoTest's own setUp() restores the
 * class-level JIT mode between tests.
 */
public class NotNullJitModeMatrixTest extends AbstractCairoTest {

    private static final int[] JIT_MODES = {
            SqlJitMode.JIT_MODE_DISABLED,
            SqlJitMode.JIT_MODE_FORCE_SCALAR,
            SqlJitMode.JIT_MODE_ENABLED
    };

    @Test
    public void testAvx2InclusiveIntComparisonBodyAndTail() throws Exception {
        // C6: the AVX2 inclusive integer kernels must not mask the sentinel bit pattern
        // out when null checks are off (NOT NULL column). Rows are laid out so sentinels
        // land both inside full 8-lane vectors and in the scalar tail.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT NOT NULL, i INT NOT NULL)");
            StringBuilder insert = new StringBuilder("INSERT INTO t VALUES ");
            for (int id = 1; id <= 21; id++) {
                if (id > 1) {
                    insert.append(", ");
                }
                // sentinel at id 2 (vector body) and id 21 (scalar tail); -5 at id 5; 0 at id 6
                int value = switch (id) {
                    case 2, 21 -> Integer.MIN_VALUE;
                    case 5 -> -5;
                    case 6 -> 0;
                    default -> id * 10;
                };
                insert.append('(').append(id).append(", ").append(value).append(')');
            }
            execute(insert.toString());

            // sentinel-as-data: both INT_MIN rows satisfy i <= 0 in every mode
            assertAcrossJitModes(
                    "SELECT count() FROM t WHERE i <= 0",
                    "count\n4\n"
            );
            assertAcrossJitModes(
                    "SELECT id FROM t WHERE i <= 0 ORDER BY id",
                    "id\n2\n5\n6\n21\n"
            );
            assertAcrossJitModes(
                    "SELECT count() FROM t WHERE i >= 0",
                    "count\n18\n"
            );
            assertUsesCompiledFilter("SELECT id FROM t WHERE i <= 0");
        });
    }

    @Test
    public void testDerivedDoubleOverflowComparisonAgreesAcrossJitModes() throws Exception {
        // C5: a derived double expression can overflow to Infinity even when every source
        // column is NOT NULL; the comparison must treat the non-finite RESULT as NULL in
        // every mode, exactly as the interpreted filter does.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT NOT NULL, d DOUBLE NOT NULL)");
            StringBuilder insert = new StringBuilder("INSERT INTO t VALUES ");
            for (int id = 1; id <= 20; id++) {
                if (id > 1) {
                    insert.append(", ");
                }
                insert.append('(').append(id).append(", ").append(id).append(".5)");
            }
            execute(insert.toString());

            assertAcrossJitModes(
                    "SELECT count() FROM t WHERE d * 1e308 * 1e308 > 0.0",
                    "count\n0\n"
            );
            assertUsesCompiledFilter("SELECT count() FROM t WHERE d * 1e308 * 1e308 > 0.0");
        });
    }

    @Test
    public void testDivisionByMinusOneOnNotNullSentinelAcrossJitModes() throws Exception {
        // P0 regression pin: x86 idiv raises #DE (a fatal SIGFPE inside JIT-generated code)
        // on INT_MIN / -1 and LONG_MIN / -1, because the quotient 2^31 / 2^63 does not fit
        // the destination register. On a NOT NULL column the sentinel bit pattern is DATA,
        // so the dividend carries no null check and reaches idiv unguarded. Java defines
        // Integer.MIN_VALUE / -1 == Integer.MIN_VALUE == INT_NULL, so both paths must
        // produce the NULL sentinel and the enclosing comparison must drop the row.
        // Row counts put a sentinel both inside a full AVX2 vector and in the scalar tail.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT NOT NULL, i INT NOT NULL, d INT)");
            StringBuilder insertInt = new StringBuilder("INSERT INTO t VALUES ");
            for (int id = 1; id <= 21; id++) {
                if (id > 1) {
                    insertInt.append(", ");
                }
                // sentinel at id 2 (vector body, 8 i32 lanes per vector) and id 21 (scalar tail)
                String value = switch (id) {
                    case 2, 21 -> "-2147483648";
                    case 5 -> "-3";
                    case 7 -> "-7";
                    default -> Integer.toString(id * 10);
                };
                // id 7 is the nullable-divisor control: a genuine NULL divisor yields NULL
                String divisor = id == 7 ? "NULL" : "-1";
                insertInt.append('(').append(id).append(", ").append(value).append(", ").append(divisor).append(')');
            }
            execute(insertInt.toString());

            // never-null divisor constant: check_lhs and check_rhs are both false
            assertAcrossJitModes("SELECT id FROM t WHERE i / -1 > 0 ORDER BY id", "id\n5\n7\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE i / -1 > 0", "count\n2\n");
            // nullable divisor column holding -1: check_rhs is true, the dividend stays unchecked
            assertAcrossJitModes("SELECT id FROM t WHERE i / d > 0 ORDER BY id", "id\n5\n");
            assertUsesCompiledFilter("SELECT id FROM t WHERE i / -1 > 0");
            assertUsesCompiledFilter("SELECT id FROM t WHERE i / d > 0");

            execute("CREATE TABLE l (id INT NOT NULL, v LONG NOT NULL, d LONG)");
            StringBuilder insertLong = new StringBuilder("INSERT INTO l VALUES ");
            for (int id = 1; id <= 21; id++) {
                if (id > 1) {
                    insertLong.append(", ");
                }
                // sentinel at id 2 (vector body, 4 i64 lanes per vector) and id 21 (scalar tail)
                String value = switch (id) {
                    case 2, 21 -> "CAST(-9223372036854775807 AS LONG) - 1";
                    case 5 -> "-3";
                    case 7 -> "-7";
                    default -> Integer.toString(id * 10);
                };
                String divisor = id == 7 ? "NULL" : "-1";
                insertLong.append('(').append(id).append(", ").append(value).append(", ").append(divisor).append(')');
            }
            execute(insertLong.toString());

            assertAcrossJitModes("SELECT id FROM l WHERE v / -1 > 0 ORDER BY id", "id\n5\n7\n");
            assertAcrossJitModes("SELECT count() FROM l WHERE v / -1 > 0", "count\n2\n");
            assertAcrossJitModes("SELECT id FROM l WHERE v / d > 0 ORDER BY id", "id\n5\n");
            assertUsesCompiledFilter("SELECT id FROM l WHERE v / -1 > 0");
            assertUsesCompiledFilter("SELECT id FROM l WHERE v / d > 0");

            // nullable control: a genuine NULL dividend is NULL, not data, in every mode
            execute("CREATE TABLE n (id INT NOT NULL, i INT, v LONG)");
            execute("INSERT INTO n VALUES (1, NULL, NULL), (2, -3, -3), (3, 7, 7)");
            assertAcrossJitModes("SELECT id FROM n WHERE i / -1 > 0 ORDER BY id", "id\n2\n");
            assertAcrossJitModes("SELECT id FROM n WHERE v / -1 > 0 ORDER BY id", "id\n2\n");
        });
    }

    @Test
    public void testIsNullOnNotNullDateAcrossJitModes() throws Exception {
        // R2: IS NULL on a NOT NULL DATE column must fold to constant false at compile
        // time; the stored sentinel bit pattern is data, not NULL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v DATE)");
            execute("INSERT INTO t VALUES (NULL), (42), (1000)");
            execute("ALTER TABLE t ALTER COLUMN v SET NOT NULL");

            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NULL", "count\n0\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NOT NULL", "count\n3\n");

            // nullable control keeps genuine NULL visible in every mode
            execute("CREATE TABLE n (v DATE)");
            execute("INSERT INTO n VALUES (NULL), (42), (1000)");
            assertAcrossJitModes("SELECT count() FROM n WHERE v IS NULL", "count\n1\n");
            assertAcrossJitModes("SELECT count() FROM n WHERE v IS NOT NULL", "count\n2\n");
        });
    }

    @Test
    public void testIsNullOnNotNullIPv4AcrossJitModes() throws Exception {
        // R2: IS NULL on a NOT NULL IPv4 column resolves through the string-constant
        // overload; it must still fold to constant false when the column is NOT NULL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v IPV4)");
            execute("INSERT INTO t VALUES ('0.0.0.0'), ('1.2.3.4'), ('5.6.7.8')");
            execute("ALTER TABLE t ALTER COLUMN v SET NOT NULL");

            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NULL", "count\n0\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NOT NULL", "count\n3\n");

            execute("CREATE TABLE n (v IPV4)");
            execute("INSERT INTO n VALUES (NULL), ('1.2.3.4'), ('5.6.7.8')");
            assertAcrossJitModes("SELECT count() FROM n WHERE v IS NULL", "count\n1\n");
            assertAcrossJitModes("SELECT count() FROM n WHERE v IS NOT NULL", "count\n2\n");
        });
    }

    @Test
    public void testMatrixDate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v DATE)");
            execute("INSERT INTO t VALUES (NULL), (-1), (0), (7)");
            execute("ALTER TABLE t ALTER COLUMN v SET NOT NULL");

            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NULL", "count\n0\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NOT NULL", "count\n4\n");
            // same-width DATE comparison: sentinel is the most negative date, i.e. data
            assertAcrossJitModes("SELECT count() FROM t WHERE v <= CAST(0 AS DATE)", "count\n3\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v < CAST(0 AS DATE)", "count\n2\n");
            // numeric spelling routes through the LONG comparison
            assertAcrossJitModes("SELECT count() FROM t WHERE v <= 0", "count\n3\n");

            execute("CREATE TABLE n (v DATE)");
            execute("INSERT INTO n VALUES (NULL), (-1), (0), (7)");
            assertAcrossJitModes("SELECT count() FROM n WHERE v IS NULL", "count\n1\n");
            assertAcrossJitModes("SELECT count() FROM n WHERE v <= CAST(0 AS DATE)", "count\n2\n");
            assertAcrossJitModes("SELECT count() FROM n WHERE v <= 0", "count\n2\n");
        });
    }

    @Test
    public void testMatrixDouble() throws Exception {
        assertMemoryLeak(() -> {
            // NaN stored as data on a NOT NULL column: IS NULL still answers false, and
            // IEEE ordering excludes NaN from < / <= as DATA in every mode alike.
            execute("CREATE TABLE t (v DOUBLE NOT NULL)");
            execute("INSERT INTO t VALUES ('NaN'::double), (-1.5), (0.0), (7.5)");

            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NULL", "count\n0\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NOT NULL", "count\n4\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v <= 0.0", "count\n2\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v <= 0", "count\n2\n");

            execute("CREATE TABLE n (v DOUBLE)");
            execute("INSERT INTO n VALUES (NULL), (-1.5), (0.0), (7.5)");
            assertAcrossJitModes("SELECT count() FROM n WHERE v IS NULL", "count\n1\n");
            assertAcrossJitModes("SELECT count() FROM n WHERE v <= 0.0", "count\n2\n");
        });
    }

    @Test
    public void testMatrixFloat() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v FLOAT NOT NULL)");
            execute("INSERT INTO t VALUES ('NaN'::float), (-1.5), (0.0), (7.5)");

            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NULL", "count\n0\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NOT NULL", "count\n4\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v <= 0.0", "count\n2\n");

            execute("CREATE TABLE n (v FLOAT)");
            execute("INSERT INTO n VALUES (NULL), (-1.5), (0.0), (7.5)");
            assertAcrossJitModes("SELECT count() FROM n WHERE v IS NULL", "count\n1\n");
            assertAcrossJitModes("SELECT count() FROM n WHERE v <= 0.0", "count\n2\n");
        });
    }

    @Test
    public void testMatrixInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v INT NOT NULL)");
            execute("INSERT INTO t VALUES (-2147483648), (-1), (0), (7)");

            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NULL", "count\n0\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NOT NULL", "count\n4\n");
            // same-width: the sentinel bit pattern is data
            assertAcrossJitModes("SELECT count() FROM t WHERE v <= 0", "count\n3\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v < 0", "count\n2\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v > 0", "count\n1\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v >= 0", "count\n2\n");
            // widened: PD's C10 agreement pin, now across every mode
            assertAcrossJitModes("SELECT count() FROM t WHERE v <= 0L", "count\n3\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v < 0L", "count\n2\n");
            assertUsesCompiledFilter("SELECT count() FROM t WHERE v <= 0");

            execute("CREATE TABLE n (v INT)");
            execute("INSERT INTO n VALUES (NULL), (-1), (0), (7)");
            assertAcrossJitModes("SELECT count() FROM n WHERE v IS NULL", "count\n1\n");
            assertAcrossJitModes("SELECT count() FROM n WHERE v <= 0", "count\n2\n");
            assertAcrossJitModes("SELECT count() FROM n WHERE v < 0", "count\n1\n");
            assertAcrossJitModes("SELECT count() FROM n WHERE v <= 0L", "count\n2\n");
        });
    }

    @Test
    public void testMatrixIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v IPV4)");
            execute("INSERT INTO t VALUES ('0.0.0.0'), ('0.0.0.1'), ('1.2.3.4')");
            execute("ALTER TABLE t ALTER COLUMN v SET NOT NULL");

            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NULL", "count\n0\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NOT NULL", "count\n3\n");
            // 0.0.0.0 is data on a NOT NULL column: it orders below every other address
            assertAcrossJitModes("SELECT count() FROM t WHERE v <= '0.0.0.1'", "count\n2\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v < '0.0.0.1'", "count\n1\n");

            execute("CREATE TABLE n (v IPV4)");
            execute("INSERT INTO n VALUES (NULL), ('0.0.0.1'), ('1.2.3.4')");
            assertAcrossJitModes("SELECT count() FROM n WHERE v IS NULL", "count\n1\n");
            assertAcrossJitModes("SELECT count() FROM n WHERE v <= '0.0.0.1'", "count\n1\n");
        });
    }

    @Test
    public void testMatrixLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v LONG NOT NULL)");
            execute("INSERT INTO t VALUES (CAST(-9223372036854775807 AS LONG) - 1), (-1), (0), (7)");

            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NULL", "count\n0\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NOT NULL", "count\n4\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v <= 0", "count\n3\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v < 0", "count\n2\n");
            // widened to double
            assertAcrossJitModes("SELECT count() FROM t WHERE v <= 0.1", "count\n3\n");
            assertUsesCompiledFilter("SELECT count() FROM t WHERE v <= 0");

            execute("CREATE TABLE n (v LONG)");
            execute("INSERT INTO n VALUES (NULL), (-1), (0), (7)");
            assertAcrossJitModes("SELECT count() FROM n WHERE v IS NULL", "count\n1\n");
            assertAcrossJitModes("SELECT count() FROM n WHERE v <= 0", "count\n2\n");
            assertAcrossJitModes("SELECT count() FROM n WHERE v <= 0.1", "count\n2\n");
        });
    }

    @Test
    public void testMatrixNonJitTypesStayModeConsistent() throws Exception {
        // UUID / LONG256 / GEOHASH never reach the compiled filter, so the matrix
        // requirement collapses to: IS [NOT] NULL answers the same in every mode and
        // honours NOT NULL sentinel-as-data reclassification.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (u UUID, h LONG256, g GEOHASH(30b))");
            execute("INSERT INTO t VALUES (NULL, NULL, NULL)");
            execute("INSERT INTO t VALUES ('11111111-1111-1111-1111-111111111111', CAST('0x01' AS LONG256), #sp052w92)");
            execute("ALTER TABLE t ALTER COLUMN u SET NOT NULL");
            execute("ALTER TABLE t ALTER COLUMN h SET NOT NULL");
            execute("ALTER TABLE t ALTER COLUMN g SET NOT NULL");

            assertAcrossJitModes("SELECT count() FROM t WHERE u IS NULL", "count\n0\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE u IS NOT NULL", "count\n2\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE h IS NULL", "count\n0\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE h IS NOT NULL", "count\n2\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE g IS NULL", "count\n0\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE g IS NOT NULL", "count\n2\n");
        });
    }

    @Test
    public void testMatrixTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // non-designated TIMESTAMP; sentinel via reclassification
            execute("CREATE TABLE t (v TIMESTAMP)");
            execute("INSERT INTO t VALUES (NULL), (-1), (0), (7)");
            execute("ALTER TABLE t ALTER COLUMN v SET NOT NULL");

            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NULL", "count\n0\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v IS NOT NULL", "count\n4\n");
            // the sentinel is the most negative timestamp: it is data and orders first
            assertAcrossJitModes("SELECT count() FROM t WHERE v <= 0", "count\n3\n");
            assertAcrossJitModes("SELECT count() FROM t WHERE v < CAST(0 AS TIMESTAMP)", "count\n2\n");

            execute("CREATE TABLE n (v TIMESTAMP)");
            execute("INSERT INTO n VALUES (NULL), (-1), (0), (7)");
            assertAcrossJitModes("SELECT count() FROM n WHERE v IS NULL", "count\n1\n");
            assertAcrossJitModes("SELECT count() FROM n WHERE v <= 0", "count\n2\n");
            assertAcrossJitModes("SELECT count() FROM n WHERE v < CAST(0 AS TIMESTAMP)", "count\n1\n");
        });
    }


    @Test
    public void testMixedNullabilityDoubleComparisonAgreesAcrossJitModes() throws Exception {
        // C5: NOT NULL double column holding Infinity as data, compared against a NULLABLE
        // column. Only the nullable operand may be null-checked; the Infinity rows are data
        // and must come back in every mode.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT NOT NULL, d DOUBLE, e DOUBLE)");
            StringBuilder insert = new StringBuilder("INSERT INTO t VALUES ");
            for (int id = 1; id <= 20; id++) {
                if (id > 1) {
                    insert.append(", ");
                }
                if (id == 1 || id == 18) {
                    insert.append('(').append(id).append(", 'Infinity'::double, 1.0)");
                } else {
                    insert.append('(').append(id).append(", 0.5, 1.0)");
                }
            }
            execute(insert.toString());
            execute("ALTER TABLE t ALTER COLUMN d SET NOT NULL");

            assertAcrossJitModes("SELECT id FROM t WHERE d > e ORDER BY id", "id\n1\n18\n");

            // nullable control: on a NULLABLE d the same non-finite values read as NULL
            execute("CREATE TABLE n (id INT NOT NULL, d DOUBLE, e DOUBLE)");
            execute("INSERT INTO n VALUES (1, 'Infinity'::double, 1.0), (2, 0.5, 1.0), (3, 5.0, 1.0)");
            assertAcrossJitModes("SELECT id FROM n WHERE d > e ORDER BY id", "id\n3\n");
            assertUsesCompiledFilter("SELECT id FROM t WHERE d > e");
        });
    }

    @Test
    public void testSameWidthComparisonSeesIntSentinelAsData() throws Exception {
        // PB2: `i < 0` (INT vs INT) must see the stored sentinel bit pattern as data on a
        // NOT NULL column, in the interpreted filter as well as in both compiled modes.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (i INT NOT NULL)");
            execute("INSERT INTO t VALUES (-2147483648), (-1), (7)");

            assertAcrossJitModes("SELECT count() FROM t WHERE i < 0", "count\n2\n");
            // PD's widened pin: both spellings agree in every mode
            assertAcrossJitModes("SELECT count() FROM t WHERE i < 0L", "count\n2\n");

            execute("CREATE TABLE n (i INT)");
            execute("INSERT INTO n VALUES (NULL), (-1), (7)");
            assertAcrossJitModes("SELECT count() FROM n WHERE i < 0", "count\n1\n");
            assertAcrossJitModes("SELECT count() FROM n WHERE i < 0L", "count\n1\n");
        });
    }

    private void assertAcrossJitModes(String sql, String expected) throws Exception {
        for (int mode : JIT_MODES) {
            sqlExecutionContext.setJitMode(mode);
            try {
                sink.clear();
                printSql(sql);
                TestUtils.assertEquals(expected, sink);
            } catch (AssertionError e) {
                throw new AssertionError("jit mode " + SqlJitMode.toString(mode) + " for [" + sql + "]: " + e.getMessage(), e);
            }
        }
    }

    private void assertUsesCompiledFilter(String sql) throws Exception {
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        try (RecordCursorFactory factory = select(sql)) {
            Assert.assertTrue("expected compiled filter for: " + sql, factory.usesCompiledFilter());
        }
    }
}
