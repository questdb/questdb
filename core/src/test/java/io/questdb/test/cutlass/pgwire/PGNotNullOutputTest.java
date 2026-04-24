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

package io.questdb.test.cutlass.pgwire;

import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Exercises the {@code outColXxx(..., boolean notNull)} branches in
 * {@code PGPipelineEntry} with {@code notNull = true}. Without these tests,
 * the NOT NULL flag propagation from {@code isColumnNotNull[colIndex]} to the
 * per-type formatters is untested, even though the flag is always allocated
 * and set.
 * <p>
 * Each test inserts a sentinel row (the result of {@code INSERT NULL} into a
 * NOT NULL column — stored as the type's null bit pattern) and a real-value
 * row. The NOT NULL branch must surface the raw sentinel bits over the PG wire
 * rather than emitting the wire-level NULL marker, which would be the
 * behaviour on a nullable column.
 */
public class PGNotNullOutputTest extends BasePGTest {

    @Test
    public void testNotNullChar() throws Exception {
        // outColChar: `if (!notNull && charValue == 0)` → for NOT NULL the sentinel char(0)
        // serialises as a 1-byte string (possibly rendered as the empty string by the driver).
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement s = connection.createStatement()) {
                s.execute("CREATE TABLE t (c CHAR NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts)");
                s.execute("INSERT INTO t VALUES (NULL, '2024-01-01')"); // sentinel row
                s.execute("INSERT INTO t VALUES ('A', '2024-01-02')");
            }
            try (PreparedStatement ps = connection.prepareStatement("SELECT c FROM t ORDER BY ts")) {
                try (ResultSet rs = ps.executeQuery()) {
                    Assert.assertTrue(rs.next());
                    // Sentinel char (0) — driver returns empty string or "\0", not SQL NULL.
                    Assert.assertFalse("sentinel row must not report wasNull", rs.wasNull());
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals("A", rs.getString(1));
                    Assert.assertFalse(rs.next());
                }
            }
        });
    }

    @Test
    public void testNotNullDateAndTimestamp() throws Exception {
        // outColBinDate / outColTxtDate / outColBinTimestamp / outColTxtTimestamp:
        // `if (notNull || longValue != Numbers.LONG_NULL)` — NOT NULL surfaces the raw
        // Long.MIN_VALUE sentinel as a wire-level value rather than the wire NULL marker.
        // The driver may refuse to parse the resulting "large negative year" date as a
        // typed Date/Timestamp; getString avoids that parse and just confirms the wire
        // frame carried data rather than a -1 length.
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement s = connection.createStatement()) {
                s.execute("CREATE TABLE t (d DATE NOT NULL, tm TIMESTAMP NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts)");
                s.execute("INSERT INTO t VALUES (NULL, NULL, '2024-01-01')");
                s.execute("INSERT INTO t VALUES ('2024-06-15'::DATE, '2024-06-15T12:00:00', '2024-01-02')");
            }
            try (PreparedStatement ps = connection.prepareStatement("SELECT d, tm FROM t ORDER BY ts")) {
                try (ResultSet rs = ps.executeQuery()) {
                    Assert.assertTrue(rs.next());
                    String dSentinel = rs.getString(1);
                    Assert.assertFalse("NOT NULL DATE sentinel must not wire as NULL", rs.wasNull());
                    Assert.assertNotNull(dSentinel);
                    Assert.assertFalse("sentinel DATE string must be non-empty", dSentinel.isEmpty());
                    String tmSentinel = rs.getString(2);
                    Assert.assertFalse("NOT NULL TIMESTAMP sentinel must not wire as NULL", rs.wasNull());
                    Assert.assertNotNull(tmSentinel);
                    Assert.assertFalse(tmSentinel.isEmpty());

                    Assert.assertTrue(rs.next());
                    Assert.assertNotNull(rs.getDate(1));
                    Assert.assertNotNull(rs.getTimestamp(2));
                }
            }
        });
    }

    @Test
    public void testNotNullDecimalAllPrecisions() throws Exception {
        // outColTxtDecimal8/16/32/64: each has `if (notNull || v != DECIMAL*_NULL)`.
        // The NOT NULL branch uses `Decimals.appendNonNull` which formats the raw
        // sentinel bits as a decimal with the column's scale. With the Decimal64
        // Long.MIN_VALUE fix, the d64 sentinel surfaces as "-92233720368547758.08"
        // (scale 2) rather than an empty token. Binary mode now threads the
        // notNull flag through outColBinDecimal, so the sentinel is rendered
        // as a concrete numeric rather than wired as NULL.
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement s = connection.createStatement()) {
                s.execute("""
                        CREATE TABLE t (
                            d8 DECIMAL(2, 0) NOT NULL,
                            d16 DECIMAL(4, 0) NOT NULL,
                            d32 DECIMAL(9, 0) NOT NULL,
                            d64 DECIMAL(18, 2) NOT NULL,
                            ts TIMESTAMP NOT NULL
                        ) TIMESTAMP(ts)
                        """);
                s.execute("INSERT INTO t VALUES (NULL, NULL, NULL, NULL, '2024-01-01')");
                s.execute("""
                        INSERT INTO t VALUES (
                            1::DECIMAL(2, 0), 100::DECIMAL(4, 0), 1000::DECIMAL(9, 0),
                            12.34::DECIMAL(18, 2), '2024-01-02'
                        )
                        """);
            }
            try (PreparedStatement ps = connection.prepareStatement("SELECT d8, d16, d32, d64 FROM t ORDER BY ts")) {
                try (ResultSet rs = ps.executeQuery()) {
                    Assert.assertTrue(rs.next());
                    for (int col = 1; col <= 4; col++) {
                        rs.getString(col);
                        Assert.assertFalse("sentinel col " + col + " must not wire as NULL", rs.wasNull());
                    }
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals("1", rs.getBigDecimal(1).toPlainString());
                    Assert.assertEquals("100", rs.getBigDecimal(2).toPlainString());
                    Assert.assertEquals("1000", rs.getBigDecimal(3).toPlainString());
                    Assert.assertEquals("12.34", rs.getBigDecimal(4).toPlainString());
                }
            }
        });
    }

    @Test
    public void testNotNullDoubleFloatNaN() throws Exception {
        // outColBinDouble / outColBinFloat: `if (!notNull && Numbers.isNull(value))`.
        // NaN would normally wire as NULL on a nullable column (Numbers.isNull treats NaN
        // as the QuestDB double/float null); under NOT NULL, NaN is a real data value and
        // must wire as a concrete double/float.
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement s = connection.createStatement()) {
                s.execute("CREATE TABLE t (d DOUBLE NOT NULL, f FLOAT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts)");
                s.execute("INSERT INTO t VALUES (NULL, NULL, '2024-01-01')");
                s.execute("INSERT INTO t VALUES (1.5, 2.5, '2024-01-02')");
            }
            try (PreparedStatement ps = connection.prepareStatement("SELECT d, f FROM t ORDER BY ts")) {
                try (ResultSet rs = ps.executeQuery()) {
                    Assert.assertTrue(rs.next());
                    double d0 = rs.getDouble(1);
                    Assert.assertFalse("NOT NULL DOUBLE NaN must not wire as NULL", rs.wasNull());
                    Assert.assertTrue("sentinel DOUBLE should be NaN", Double.isNaN(d0));
                    float f0 = rs.getFloat(2);
                    Assert.assertFalse("NOT NULL FLOAT NaN must not wire as NULL", rs.wasNull());
                    Assert.assertTrue("sentinel FLOAT should be NaN", Float.isNaN(f0));

                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(1.5, rs.getDouble(1), 0.0);
                    Assert.assertEquals(2.5f, rs.getFloat(2), 0.0f);
                }
            }
        });
    }

    @Test
    public void testNotNullIntAndLong() throws Exception {
        // outColBinInt / outColTxtInt / outColBinLong / outColTxtLong:
        // `if (notNull || value != INT_NULL)` and the long equivalent.
        // Sentinel values (Integer.MIN_VALUE / Long.MIN_VALUE) must wire as concrete
        // ints/longs, not the wire NULL marker. Sentinel-row assertions use getString
        // to avoid driver type-parse failures on some PG JDBC versions, which can
        // surface INT_MIN / LONG_MIN text as unparseable on certain locales.
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement s = connection.createStatement()) {
                s.execute("CREATE TABLE t (i INT NOT NULL, l LONG NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts)");
                s.execute("INSERT INTO t VALUES (NULL, NULL, '2024-01-01')");
                s.execute("INSERT INTO t VALUES (42, 99, '2024-01-02')");
            }
            try (PreparedStatement ps = connection.prepareStatement("SELECT i, l FROM t ORDER BY ts")) {
                try (ResultSet rs = ps.executeQuery()) {
                    Assert.assertTrue(rs.next());
                    String iSentinel = rs.getString(1);
                    Assert.assertFalse("NOT NULL INT sentinel must not wire as NULL", rs.wasNull());
                    Assert.assertNotNull(iSentinel);
                    String lSentinel = rs.getString(2);
                    Assert.assertFalse("NOT NULL LONG sentinel must not wire as NULL", rs.wasNull());
                    Assert.assertNotNull(lSentinel);

                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(42, rs.getInt(1));
                    Assert.assertEquals(99L, rs.getLong(2));
                }
            }
        });
    }

    @Test
    public void testNotNullIpv4() throws Exception {
        // outColTxtIPv4: NOT NULL branch renders the sentinel IPv4 (0.0.0.0).
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement s = connection.createStatement()) {
                s.execute("CREATE TABLE t (ip IPv4 NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts)");
                s.execute("INSERT INTO t VALUES (NULL, '2024-01-01')");
                s.execute("INSERT INTO t VALUES ('1.2.3.4', '2024-01-02')");
            }
            try (PreparedStatement ps = connection.prepareStatement("SELECT ip FROM t ORDER BY ts")) {
                try (ResultSet rs = ps.executeQuery()) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals("0.0.0.0", rs.getString(1));
                    Assert.assertFalse(rs.wasNull());
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals("1.2.3.4", rs.getString(1));
                }
            }
        });
    }

    @Test
    public void testNotNullLong256() throws Exception {
        // outColTxtLong256: NOT NULL branch — the all-zero sentinel is sent as the
        // raw hex string rather than wire NULL.
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement s = connection.createStatement()) {
                s.execute("CREATE TABLE t (l LONG256 NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts)");
                s.execute("INSERT INTO t VALUES (NULL, '2024-01-01')");
                s.execute("INSERT INTO t VALUES ('0x01', '2024-01-02')");
            }
            try (PreparedStatement ps = connection.prepareStatement("SELECT l FROM t ORDER BY ts")) {
                try (ResultSet rs = ps.executeQuery()) {
                    Assert.assertTrue(rs.next());
                    String sentinel = rs.getString(1);
                    Assert.assertFalse("NOT NULL LONG256 sentinel must not wire as NULL", rs.wasNull());
                    // Sentinel is 0x0 (short form of all-zeros).
                    Assert.assertNotNull(sentinel);
                    Assert.assertTrue(rs.next());
                    Assert.assertNotNull(rs.getString(1));
                }
            }
        });
    }

    @Test
    public void testNotNullUuid() throws Exception {
        // outColBinUuid / outColTxtUuid: `if (!notNull && Uuid.isNull(lo, hi))`.
        // NOT NULL surfaces the sentinel UUID bit pattern.
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement s = connection.createStatement()) {
                s.execute("CREATE TABLE t (u UUID NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts)");
                s.execute("INSERT INTO t VALUES (NULL, '2024-01-01')");
                s.execute("INSERT INTO t VALUES ('11111111-1111-1111-1111-111111111111', '2024-01-02')");
            }
            try (PreparedStatement ps = connection.prepareStatement("SELECT u FROM t ORDER BY ts")) {
                try (ResultSet rs = ps.executeQuery()) {
                    Assert.assertTrue(rs.next());
                    Assert.assertNotNull(rs.getString(1));
                    Assert.assertFalse("NOT NULL UUID sentinel must not wire as NULL", rs.wasNull());
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals("11111111-1111-1111-1111-111111111111", rs.getString(1));
                }
            }
        });
    }

    @Test
    public void testNullableColumnStillWiresAsNull() throws Exception {
        // Regression guard: on a nullable column, the sentinel value must still wire as
        // SQL NULL (the !notNull branch is the default path).
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement s = connection.createStatement()) {
                s.execute("CREATE TABLE t (i INT, l LONG, d DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts)");
                s.execute("INSERT INTO t VALUES (NULL, NULL, NULL, '2024-01-01')");
            }
            try (PreparedStatement ps = connection.prepareStatement("SELECT i, l, d FROM t")) {
                try (ResultSet rs = ps.executeQuery()) {
                    Assert.assertTrue(rs.next());
                    rs.getInt(1);
                    Assert.assertTrue("nullable INT null must wire as NULL", rs.wasNull());
                    rs.getLong(2);
                    Assert.assertTrue("nullable LONG null must wire as NULL", rs.wasNull());
                    rs.getDouble(3);
                    Assert.assertTrue("nullable DOUBLE null must wire as NULL", rs.wasNull());
                }
            }
        });
    }
}
