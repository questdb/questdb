/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.Decimal256;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class PGDecimalsTest extends BasePGTest {

    // Test boundary values for each decimal type
    @Test
    public void testBoundaryValues() throws Exception {
        // DECIMAL8 max (precision 2)
        assertDecimalConversion("99", 2, 0);
        assertDecimalConversion("-99", 2, 0);

        // DECIMAL16 max (precision 4)
        assertDecimalConversion("9999", 4, 0);
        assertDecimalConversion("-9999", 4, 0);

        // DECIMAL32 max (precision 9)
        assertDecimalConversion("999999999", 9, 0);
        assertDecimalConversion("-999999999", 9, 0);

        // DECIMAL64 max (precision 18)
        assertDecimalConversion("999999999999999999", 18, 0);
        assertDecimalConversion("-999999999999999999", 18, 0);

        // DECIMAL128 max (precision 38)
        assertDecimalConversion("99999999999999999999999999999999999999", 38, 0);
        assertDecimalConversion("-99999999999999999999999999999999999999", 38, 0);
    }

    // Test DECIMAL128 (precision 19-38)
    @Test
    public void testDecimal128_NineteenDigits() throws Exception {
        assertDecimalConversion("9999999999999999999", 19, 0);
        assertDecimalConversion("-9999999999999999999", 19, 0);
    }

    @Test
    public void testDecimal128_ThirtyDigits() throws Exception {
        assertDecimalConversion("999999999999999999999999999999", 30, 0);
        assertDecimalConversion("-999999999999999999999999999999", 30, 0);
    }

    @Test
    public void testDecimal128_ThirtyEightDigits() throws Exception {
        assertDecimalConversion("99999999999999999999999999999999999999", 38, 0);
        assertDecimalConversion("-99999999999999999999999999999999999999", 38, 0);
    }

    @Test
    public void testDecimal128_TwentyFiveDigits() throws Exception {
        assertDecimalConversion("9999999999999999999999999", 25, 0);
        assertDecimalConversion("-9999999999999999999999999", 25, 0);
    }

    @Test
    public void testDecimal128_WithScale() throws Exception {
        assertDecimalConversion("99999999999999999.99", 19, 2);
        assertDecimalConversion("-99999999999999999.99", 19, 2);
        assertDecimalConversion("9999999999999999999999999.9999999999", 35, 10);
        assertDecimalConversion("99999999999999999999999999999999.999999", 38, 6);
    }

    @Test
    public void testDecimal16_FourDigits() throws Exception {
        assertDecimalConversion("9999", 4, 0);
        assertDecimalConversion("-9999", 4, 0);
    }

    // Test DECIMAL16 (precision 3-4)
    @Test
    public void testDecimal16_ThreeDigits() throws Exception {
        assertDecimalConversion("999", 3, 0);
        assertDecimalConversion("-999", 3, 0);
    }

    @Test
    public void testDecimal16_WithScale() throws Exception {
        assertDecimalConversion("99.99", 4, 2);
        assertDecimalConversion("-99.99", 4, 2);
        assertDecimalConversion("9.999", 4, 3);
    }

    @Test
    public void testDecimal256_FiftyDigits() throws Exception {
        assertDecimalConversion("99999999999999999999999999999999999999999999999999", 50, 0);
        assertDecimalConversion("-99999999999999999999999999999999999999999999999999", 50, 0);
    }

    @Test
    public void testDecimal256_MaxPrecision() throws Exception {
        // MAX_PRECISION is 76
        assertDecimalConversion("9999999999999999999999999999999999999999999999999999999999999999999999999999", 76, 0);
        assertDecimalConversion("-9999999999999999999999999999999999999999999999999999999999999999999999999999", 76, 0);
    }

    @Test
    public void testDecimal256_SeventyDigits() throws Exception {
        assertDecimalConversion("9999999999999999999999999999999999999999999999999999999999999999999999", 70, 0);
        assertDecimalConversion("-9999999999999999999999999999999999999999999999999999999999999999999999", 70, 0);
    }

    @Test
    public void testDecimal256_SixtyDigits() throws Exception {
        assertDecimalConversion("999999999999999999999999999999999999999999999999999999999999", 60, 0);
        assertDecimalConversion("-999999999999999999999999999999999999999999999999999999999999", 60, 0);
    }

    // Test DECIMAL256 (precision 39-76)
    @Test
    public void testDecimal256_ThirtyNineDigits() throws Exception {
        assertDecimalConversion("999999999999999999999999999999999999999", 39, 0);
        assertDecimalConversion("-999999999999999999999999999999999999999", 39, 0);
    }

    @Test
    public void testDecimal256_WithScale() throws Exception {
        assertDecimalConversion("999999999999999999999999999999999999999.99", 41, 2);
        assertDecimalConversion("-999999999999999999999999999999999999999.99", 41, 2);
        assertDecimalConversion("9999999999999999999999999999999999999999999999999.9999999999", 60, 10);
        assertDecimalConversion("999999999999999999999999999999999999999999999999999999999999999999.99999999", 76, 8);
    }

    // Test DECIMAL32 (precision 5-9)
    @Test
    public void testDecimal32_FiveDigits() throws Exception {
        assertDecimalConversion("99999", 5, 0);
        assertDecimalConversion("-99999", 5, 0);
    }

    @Test
    public void testDecimal32_NineDigits() throws Exception {
        assertDecimalConversion("999999999", 9, 0);
        assertDecimalConversion("-999999999", 9, 0);
    }

    @Test
    public void testDecimal32_SevenDigits() throws Exception {
        assertDecimalConversion("9999999", 7, 0);
        assertDecimalConversion("-9999999", 7, 0);
    }

    @Test
    public void testDecimal32_WithScale() throws Exception {
        assertDecimalConversion("99999.99", 7, 2);
        assertDecimalConversion("-99999.99", 7, 2);
        assertDecimalConversion("999.99999", 8, 5);
        assertDecimalConversion("9999999.99", 9, 2);
    }

    @Test
    public void testDecimal64_EighteenDigits() throws Exception {
        assertDecimalConversion("999999999999999999", 18, 0);
        assertDecimalConversion("-999999999999999999", 18, 0);
    }

    @Test
    public void testDecimal64_FifteenDigits() throws Exception {
        assertDecimalConversion("999999999999999", 15, 0);
        assertDecimalConversion("-999999999999999", 15, 0);
    }

    // Test DECIMAL64 (precision 10-18)
    @Test
    public void testDecimal64_TenDigits() throws Exception {
        assertDecimalConversion("9999999999", 10, 0);
        assertDecimalConversion("-9999999999", 10, 0);
    }

    @Test
    public void testDecimal64_WithScale() throws Exception {
        assertDecimalConversion("9999999999.99999999", 18, 8);
        assertDecimalConversion("-9999999999.99999999", 18, 8);
        assertDecimalConversion("999999999999.999999", 18, 6);
        assertDecimalConversion("99999999999999.9999", 18, 4);
    }

    // Test DECIMAL8 (precision 1-2)
    @Test
    public void testDecimal8_SingleDigit() throws Exception {
        assertDecimalConversion("9", 1, 0);
        assertDecimalConversion("-9", 1, 0);
    }

    @Test
    public void testDecimal8_TwoDigits() throws Exception {
        assertDecimalConversion("99", 2, 0);
        assertDecimalConversion("-99", 2, 0);
    }

    @Test
    public void testDecimal8_WithScale() throws Exception {
        assertDecimalConversion("9.9", 2, 1);
        assertDecimalConversion("-9.9", 2, 1);
    }

    @Test
    public void testDecimalBind() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE tango(dec decimal(2, 0), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR");
            }
            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO tango VALUES (?, ?)")) {
                statement.setLong(1, 10);
                statement.setTimestamp(2, new java.sql.Timestamp(0));
                statement.executeUpdate();
            }
            drainWalQueue();
            try (PreparedStatement statement = connection.prepareStatement("UPDATE tango SET dec = ?")) {
                statement.setLong(1, 12);
                statement.executeUpdate();
            }
            drainWalQueue();
            try (PreparedStatement stmt = connection.prepareStatement("tango")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet("dec[NUMERIC],ts[TIMESTAMP]\n" +
                                    "12,1970-01-01 00:00:00.0\n",
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalBindBigDecimal() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE tango(dec decimal(18, 5), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR");
            }
            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO tango VALUES (?, ?)")) {
                statement.setObject(1, new BigDecimal("12345670.890"));
                statement.setTimestamp(2, new java.sql.Timestamp(0));
                statement.executeUpdate();
            }
            drainWalQueue();
            try (PreparedStatement stmt = connection.prepareStatement("tango")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet("dec[NUMERIC],ts[TIMESTAMP]\n" +
                                    "12345670.89000,1970-01-01 00:00:00.0\n",
                            sink,
                            rs
                    );
                }
            }
            try (PreparedStatement statement = connection.prepareStatement("UPDATE tango SET dec = ?")) {
                statement.setObject(1, new BigDecimal("-456.123"));
                statement.executeUpdate();
            }
            drainWalQueue();
            try (PreparedStatement stmt = connection.prepareStatement("tango")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet("dec[NUMERIC],ts[TIMESTAMP]\n" +
                                    "-456.12300,1970-01-01 00:00:00.0\n",
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalBindOverflow() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE tango(dec decimal(18, 5), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR");
            }
            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO tango VALUES (?, ?)")) {
                statement.setObject(1, new BigDecimal("10e80"));
                statement.setTimestamp(2, new java.sql.Timestamp(0));
                try {
                    statement.executeUpdate();
                    Assert.fail();
                } catch (PSQLException e) {
                    TestUtils.assertContainsEither(
                            e.getMessage(),
                            "inconvertible value",
                            "numeric is too big for a decimal",
                            "requires precision of"
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalBindScaleOverflow() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE tango(dec decimal(18, 5), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR");
            }
            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO tango VALUES (?, ?)")) {
                statement.setObject(1, new BigDecimal("1e-80"));
                statement.setTimestamp(2, new java.sql.Timestamp(0));
                try {
                    statement.executeUpdate();
                    Assert.fail();
                } catch (PSQLException e) {
                    TestUtils.assertContainsEither(
                            e.getMessage(),
                            "inconvertible value",
                            "numeric scale too big for a decimal",
                            "decimal places but scale is limited"
                    );
                }
            }
        });
    }

    // Test fractional values
    @Test
    public void testFractionalValues() throws Exception {
        assertDecimalConversion("0.1", 2, 1);
        assertDecimalConversion("0.01", 3, 2);
        assertDecimalConversion("0.001", 4, 3);
        assertDecimalConversion("0.0001", 5, 4);
        assertDecimalConversion("0.12345", 6, 5);
        assertDecimalConversion("0.123456789", 10, 9);
    }

    // Test high scale values
    @Test
    public void testHighScaleValues() throws Exception {
        assertDecimalConversion("0.99", 3, 2);
        assertDecimalConversion("0.9999", 5, 4);
        assertDecimalConversion("0.999999999", 10, 9);
        assertDecimalConversion("0.999999999999999999", 19, 18);
        assertDecimalConversion("1.23456789012345678901234567890123456789", 39, 38);
    }

    // Test mixed integer and fractional parts
    @Test
    public void testMixedValues() throws Exception {
        assertDecimalConversion("123.456", 6, 3);
        assertDecimalConversion("-123.456", 6, 3);
        assertDecimalConversion("1234567.89", 9, 2);
        assertDecimalConversion("-1234567.89", 9, 2);
        assertDecimalConversion("12345678901234567890.123456789", 30, 9);
    }

    @Test
    public void testNull() throws Exception {
        assertNull(2, 1);
        assertNull(4, 2);
        assertNull(8, 4);
        assertNull(16, 8);
        assertNull(32, 16);
        assertNull(64, 32);
    }

    @Test
    public void testNullBothWays() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (var select = connection.prepareStatement("select cast(? as decimal(15, 3))")) {
                select.setBigDecimal(1, null);
                try (var resultSet = select.executeQuery()) {
                    Assert.assertTrue(resultSet.next());
                    var bigDecimal = resultSet.getBigDecimal(1);
                    Assert.assertNull(bigDecimal);
                }
            }
        });
    }

    // Test scientific notation numbers converted to decimal
    @Test
    public void testScientificNotationValues() throws Exception {
        assertDecimalConversion("100", 3, 0); // 1e2
        assertDecimalConversion("0.01", 3, 2); // 1e-2
        assertDecimalConversion("1000000", 7, 0); // 1e6
        assertDecimalConversion("0.000001", 7, 6); // 1e-6
    }

    // Test the original small test case
    @Test
    public void testSmall() throws Exception {
        assertDecimalConversion("-123", 5, 1);
    }

    @Test
    public void testSmallValue() throws Exception {
        // We are able to convert the ''::numeric::decimal(46, 45) to a direct ''::decimal(46, 45) in simple mode.
        // Unfortunately, for other mode we're still relying on the bind variable to retrieve the expected decimal
        // type, which gives us a precision of 76 and a scale of 38.
        assertWithPgServer(CONN_AWARE_SIMPLE, (connection, binary, mode, port) -> {
            try (var select = connection.prepareStatement("select cast(? as decimal(46, 45))")) {
                select.setBigDecimal(1, new BigDecimal("1E-45"));
                try (var resultSet = select.executeQuery()) {
                    Assert.assertTrue(resultSet.next());
                    var bigDecimal = resultSet.getBigDecimal(1);
                    Assert.assertEquals("1E-45", bigDecimal.toString());
                }
            }
        });
    }

    // Test zero values with different scales
    @Test
    public void testZeroValues() throws Exception {
        assertDecimalConversion("0", 1, 0);
        assertDecimalConversion("0.0", 2, 1);
        assertDecimalConversion("0.00", 3, 2);
        assertDecimalConversion("0.000", 4, 3);
        assertDecimalConversion("0.0000", 5, 4);
        assertDecimalConversion("0.00000000", 9, 8);
    }

    private void assertDecimalConversion(CharSequence cs, int precision, int scale) throws Exception {
        Decimal256 decimal256 = new Decimal256();
        decimal256.ofString(cs, precision, scale);
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (var select = connection.prepareStatement(String.format("select cast(? as decimal(%d, %d))", precision, scale))) {
                select.setString(1, decimal256.toString());
                try (var resultSet = select.executeQuery()) {
                    Assert.assertTrue(resultSet.next());
                    var bigDecimal = resultSet.getBigDecimal(1);
                    Assert.assertEquals(decimal256, Decimal256.fromBigDecimal(bigDecimal));
                }
            }
        });
    }

    private void assertNull(int precision, int scale) throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (var select = connection.prepareStatement(String.format("select cast(null as decimal(%d, %d))", precision, scale))) {
                try (var resultSet = select.executeQuery()) {
                    Assert.assertTrue(resultSet.next());
                    var bigDecimal = resultSet.getBigDecimal(1);
                    Assert.assertNull(bigDecimal);
                }
            }
        });
    }
}
