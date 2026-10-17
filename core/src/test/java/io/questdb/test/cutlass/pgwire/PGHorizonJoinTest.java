/*+*****************************************************************************
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

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PGHorizonJoinTest extends BasePGTest {

    @Test
    public void testProjectionBareBindVariable() throws Exception {
        // A bind variable that the select list outputs as it is used to fail compilation of a
        // HORIZON JOIN, with or without aggregates. A one-character value would come back as a
        // CHAR in simple mode, where the driver inlines it as a literal.
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            createTables(connection);
            try (PreparedStatement ps = connection.prepareStatement("""
                    SELECT ? tag, t.sym, h.offset, q.bid FROM trades t
                    HORIZON JOIN quotes q ON (sym) LIST (0s, 1s) AS h
                    """)) {
                ps.setString(1, "xy");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    tag[VARCHAR],sym[VARCHAR],offset[BIGINT],bid[DOUBLE]
                                    xy,A,0,10.0
                                    xy,A,1000000,12.0
                                    xy,A,0,12.0
                                    xy,A,1000000,14.0
                                    xy,B,0,20.0
                                    xy,B,1000000,22.0
                                    """,
                            sink,
                            rs
                    );
                }
            }
            try (PreparedStatement ps = connection.prepareStatement("""
                    SELECT ? tag, count() c FROM trades t
                    HORIZON JOIN quotes q ON (sym) LIST (0s, 1s) AS h
                    """)) {
                ps.setString(1, "xy");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    tag[VARCHAR],c[BIGINT]
                                    xy,6
                                    """,
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testProjectionPreparedStatementReuse() throws Exception {
        // A prepared statement runs the same factory with new values. The right-hand filter
        // matches, then matches nothing, then matches again, and the left-hand filter changes
        // along with it.
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            createTables(connection);
            try (PreparedStatement ps = connection.prepareStatement("""
                    SELECT t.ts, t.sym, h.offset, q.bid FROM trades t
                    HORIZON JOIN (quotes WHERE venue = ?) q ON (sym) LIST (0s, 1s) AS h
                    WHERE t.price > ?
                    """)) {
                final String allV1 = """
                        ts[TIMESTAMP],sym[VARCHAR],offset[BIGINT],bid[DOUBLE]
                        2026-01-01 00:00:00.0,A,0,10.0
                        2026-01-01 00:00:00.0,A,1000000,10.0
                        2026-01-01 00:00:02.0,A,0,10.0
                        2026-01-01 00:00:02.0,A,1000000,14.0
                        2026-01-01 00:00:02.0,B,0,20.0
                        2026-01-01 00:00:02.0,B,1000000,20.0
                        """;
                assertRows(ps, "V1", 0, allV1);
                assertRows(ps, "ZZZ", 0, """
                        ts[TIMESTAMP],sym[VARCHAR],offset[BIGINT],bid[DOUBLE]
                        2026-01-01 00:00:00.0,A,0,null
                        2026-01-01 00:00:00.0,A,1000000,null
                        2026-01-01 00:00:02.0,A,0,null
                        2026-01-01 00:00:02.0,A,1000000,null
                        2026-01-01 00:00:02.0,B,0,null
                        2026-01-01 00:00:02.0,B,1000000,null
                        """);
                assertRows(ps, "V2", 10.5, """
                        ts[TIMESTAMP],sym[VARCHAR],offset[BIGINT],bid[DOUBLE]
                        2026-01-01 00:00:02.0,A,0,12.0
                        2026-01-01 00:00:02.0,A,1000000,12.0
                        2026-01-01 00:00:02.0,B,0,null
                        2026-01-01 00:00:02.0,B,1000000,22.0
                        """);
                assertRows(ps, "V1", 0, allV1);
            }
        });
    }

    private static void createTables(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY"
        )) {
            Assert.assertFalse(ps.execute());
        }
        try (PreparedStatement ps = connection.prepareStatement(
                "CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, venue SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY"
        )) {
            Assert.assertFalse(ps.execute());
        }
        try (PreparedStatement ps = connection.prepareStatement("""
                INSERT INTO trades VALUES
                    ('2026-01-01T00:00:00Z', 'A', 10),
                    ('2026-01-01T00:00:02Z', 'A', 11),
                    ('2026-01-01T00:00:02Z', 'B', 20)
                """)) {
            Assert.assertFalse(ps.execute());
        }
        try (PreparedStatement ps = connection.prepareStatement("""
                INSERT INTO quotes VALUES
                    ('2026-01-01T00:00:00Z', 'A', 'V1', 10),
                    ('2026-01-01T00:00:00Z', 'B', 'V1', 20),
                    ('2026-01-01T00:00:01Z', 'A', 'V2', 12),
                    ('2026-01-01T00:00:03Z', 'A', 'V1', 14),
                    ('2026-01-01T00:00:03Z', 'B', 'V2', 22)
                """)) {
            Assert.assertFalse(ps.execute());
        }
    }

    private void assertRows(PreparedStatement ps, String venue, double minPrice, String expected) throws Exception {
        ps.setString(1, venue);
        ps.setDouble(2, minPrice);
        try (ResultSet rs = ps.executeQuery()) {
            assertResultSet(expected, sink, rs);
        }
    }
}
