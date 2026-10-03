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
 * an explicit {@code NULL} literal on a NOT NULL column is a compile-time
 * error; an explicitly spelled sentinel bit pattern is ordinary data; a bind
 * variable set to NULL is a runtime NULL and stores the sentinel. Omitting a
 * NOT NULL column from the INSERT column list is rejected by the writer. The
 * PG wire path must match these semantics.
 */
public class PGNotNullInsertTest extends BasePGTest {

    @Test
    public void testPgInsertExplicitNullRejected() throws Exception {
        // An explicit NULL literal into a NOT NULL column is a compile-time error,
        // surfaced to the JDBC driver as an SQLException; no row lands. Mirrors the
        // SQL semantic asserted by NotNullColumnTest.testEnforceNotNullSentinelValuesAccepted.
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
                Assert.fail("Expected NOT NULL constraint violation for explicit NULL literal");
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "NOT NULL constraint violation [column=x]");
            }
            try (Statement s = connection.createStatement();
                 ResultSet rs = s.executeQuery("SELECT count() FROM pg_nn_explicit_null")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(0, rs.getLong(1));
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
        // Unlike a simple-query explicit NULL literal, which fails at compile
        // time (NOT NULL constraint violation), a bind variable set to NULL via
        // setNull() is a runtime NULL: it counts as "written to" and stores the
        // sentinel (syntactic-limit contract).
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
