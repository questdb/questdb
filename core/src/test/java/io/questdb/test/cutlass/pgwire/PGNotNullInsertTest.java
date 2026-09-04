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

import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Verifies NOT NULL constraint behavior for INSERTs that arrive over the
 * PostgreSQL wire protocol, in both simple-query and extended-query
 * (prepared statement) modes.
 * <p>
 * QuestDB NOT NULL semantic (per
 * {@code NotNullColumnTest.testEnforceNotNullSentinelValuesAccepted}):
 * NOT NULL means "the column must be written to". An explicit {@code NULL}
 * literal counts as written and is accepted -- the type's sentinel value is
 * stored. Omitting a NOT NULL column from the INSERT column list is rejected.
 * The PG wire path must match these semantics.
 */
public class PGNotNullInsertTest extends BasePGTest {

    @Test
    public void testPgInsertExplicitNullStoresSentinel() throws Exception {
        // Explicit NULL into NOT NULL column is accepted -- the type's sentinel
        // is stored. For DOUBLE that's NaN. Mirrors the SQL semantic asserted
        // by NotNullColumnTest.testEnforceNotNullSentinelValuesAccepted.
        assertWithPgServer(CONN_AWARE_SIMPLE, (connection, binary, mode, port) -> {
            try (Statement s = connection.createStatement()) {
                s.execute("""
                        CREATE TABLE pg_nn_explicit_null (
                            ts TIMESTAMP NOT NULL,
                            x DOUBLE NOT NULL
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """);
            }
            try (Statement s = connection.createStatement()) {
                s.execute("INSERT INTO pg_nn_explicit_null (ts, x) VALUES ('2024-01-01', NULL)");
            }
            try (Statement s = connection.createStatement();
                 ResultSet rs = s.executeQuery("SELECT x FROM pg_nn_explicit_null")) {
                Assert.assertTrue(rs.next());
                double x = rs.getDouble(1);
                Assert.assertTrue("NOT NULL DOUBLE sentinel must read back as NaN", Double.isNaN(x));
                Assert.assertFalse(rs.next());
            }
        });
    }

    @Test
    public void testPgInsertOmittedNotNullColumnRejected() throws Exception {
        // The NOT NULL "x" column is missing from the INSERT column list.
        // TableWriter.rowAppend() must throw, surfacing as SQLException to
        // the JDBC driver. Mirrors NotNullColumnTest.testEnforceNotNullMissingColumn.
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement s = connection.createStatement()) {
                s.execute("""
                        CREATE TABLE pg_nn_omitted (
                            ts TIMESTAMP NOT NULL,
                            x DOUBLE NOT NULL,
                            y DOUBLE
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """);
            }
            try (Statement s = connection.createStatement()) {
                s.execute("INSERT INTO pg_nn_omitted (ts, y) VALUES ('2024-01-01', 1.5)");
                Assert.fail("Expected NOT NULL constraint violation for omitted NOT NULL column");
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "NOT NULL constraint violation");
                TestUtils.assertContains(e.getMessage(), "column=x");
            }
        });
    }

    @Test
    public void testPgPreparedInsertNullBindStoresSentinel() throws Exception {
        // Extended-query bind variable set to NULL via setNull() is semantically
        // equivalent to the simple-query explicit-NULL case: it counts as
        // "written to" and stores the sentinel.
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            try (Statement s = connection.createStatement()) {
                s.execute("""
                        CREATE TABLE pg_nn_prepared_null (
                            ts TIMESTAMP NOT NULL,
                            x DOUBLE NOT NULL
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """);
            }
            try (PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO pg_nn_prepared_null (ts, x) VALUES (?, ?)"
            )) {
                ps.setTimestamp(1, new Timestamp(1_704_067_200_000L)); // 2024-01-01
                ps.setNull(2, Types.DOUBLE);
                ps.execute();
            }
            try (Statement s = connection.createStatement();
                 ResultSet rs = s.executeQuery("SELECT x FROM pg_nn_prepared_null")) {
                Assert.assertTrue(rs.next());
                double x = rs.getDouble(1);
                Assert.assertTrue("NOT NULL DOUBLE sentinel must read back as NaN", Double.isNaN(x));
                Assert.assertFalse(rs.next());
            }
        });
    }
}
