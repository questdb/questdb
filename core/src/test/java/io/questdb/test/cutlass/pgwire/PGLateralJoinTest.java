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

import io.questdb.griffin.engine.table.LateralNullCheckRecordCursorFactory;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

public class PGLateralJoinTest extends BasePGTest {

    // A prepared statement whose LATERAL body holds a RIGHT join over a computed SHORT column runs with
    // a NULL value: t.sid != 0 is false on the 0 that the join puts in the SHORT column, so it drops
    // the rows that the join NULL-extends whatever the value of t.i = ?, and the plan needs no check.
    // Trade 11 holds NULL in i, and NULL = NULL is true. The simple query mode sends NULL as a
    // constant, which the rewriter decides on when it compiles the query, so only the extended modes
    // run.
    @Test
    public void testRuntimeNullCheckNextToAComputedColumn() throws Exception {
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                createOrdersAndRefunds(statement);
                statement.execute("CREATE TABLE trades (id INT, x INT, i INT, ts TIMESTAMP) TIMESTAMP(ts)");
                statement.execute("INSERT INTO trades VALUES (10, 1, 10, 1::timestamp), (11, 2, NULL, 2::timestamp)");
            }
            final String sql = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM "
                    + "(SELECT id, x, i, CAST(id AS SHORT) sid FROM trades) t RIGHT JOIN refunds r ON t.x = r.k AND r.k = o.k "
                    + "WHERE t.i = ? AND t.sid != 0) l ORDER BY 1, 2, 3";
            final String header = "id[INTEGER],tid[INTEGER],rid[INTEGER]\n";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setNull(1, Types.INTEGER);
                try (ResultSet rs = statement.executeQuery()) {
                    assertResultSet(header + "2,11,101\n", sink, rs);
                }
                statement.setInt(1, 10);
                try (ResultSet rs = statement.executeQuery()) {
                    assertResultSet(header + "1,10,100\n", sink, rs);
                }
            }
        });
    }

    // A prepared statement whose LATERAL body holds a RIGHT join checks, at each execution, the value
    // of a CASE over a scalar sub-query, which the filter reads too. Per outer row, refund k matches
    // both trades, and t.id >= 10 keeps both. Refund 103 makes max(id) 103, so the CASE becomes NULL,
    // and the next execution fails at the CASE.
    @Test
    public void testRuntimeNullCheckOfANestedSubQuery() throws Exception {
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                createOrdersAndRefunds(statement);
                statement.execute("CREATE TABLE trades (id INT, x INT, ts TIMESTAMP) TIMESTAMP(ts)");
                statement.execute("INSERT INTO trades VALUES (10, 1, 1::timestamp), (11, 2, 2::timestamp)");
            }
            final String sql = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM "
                    + "trades t RIGHT JOIN refunds r ON r.k = o.k "
                    + "WHERE t.id >= CASE WHEN (SELECT max(id) FROM refunds) > 102 THEN NULL::int ELSE 10 END) l ORDER BY 1, 2, 3";
            final String header = "id[INTEGER],tid[INTEGER],rid[INTEGER]\n";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (int i = 0; i < 2; i++) {
                    try (ResultSet rs = statement.executeQuery()) {
                        assertResultSet(header + "1,10,100\n1,11,100\n2,10,101\n2,11,101\n3,10,102\n3,11,102\n", sink, rs);
                    }
                }
                try (Statement insert = connection.createStatement()) {
                    insert.execute("INSERT INTO refunds VALUES (103, 4, 4::timestamp)");
                }
                for (int i = 0; i < 2; i++) {
                    assertNullValueError(statement, sql.indexOf("CASE") + 1);
                }
            }
        });
    }

    // A prepared statement whose LATERAL body holds a RIGHT join checks, at each execution, the
    // values of the filter conjuncts that drop the rows the join NULL-extends only while their value
    // is not NULL. The conjuncts are ANDed, so the execution fails only when every value is NULL, at
    // the first of them. Trade 11 holds NULL in i and 10 in l: with i = NULL and l >= 10 it matches,
    // as NULL = NULL is true, while NULL >= 10 is false on the NULL-extended rows. The simple query
    // mode sends NULL as a constant, which the rewriter rejects when it compiles the query, so only
    // the extended modes run.
    @Test
    public void testRuntimeNullCheckOfAnyConjunct() throws Exception {
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                createOrdersAndRefunds(statement);
                statement.execute("CREATE TABLE trades (id INT, x INT, i INT, l LONG, ts TIMESTAMP) TIMESTAMP(ts)");
                statement.execute("INSERT INTO trades VALUES (10, 1, 10, 10, 1::timestamp), (11, 2, NULL, 10, 2::timestamp)");
            }
            final String body = "trades t RIGHT JOIN refunds r ON t.x = r.k AND r.k = {K} WHERE t.i = ? AND t.l >= ?";
            final String sql = "SELECT o.id, l.tid, l.rid FROM orders o JOIN LATERAL (SELECT t.id tid, r.id rid FROM "
                    + body.replace("{K}", "o.k") + ") l ORDER BY 1, 2, 3";
            // the body once per outer row, with the outer key as a literal
            final String reference = "SELECT * FROM ("
                    + "SELECT 1 id, tid, rid FROM (SELECT t.id tid, r.id rid FROM " + body.replace("{K}", "1") + ") UNION ALL "
                    + "SELECT 2 id, tid, rid FROM (SELECT t.id tid, r.id rid FROM " + body.replace("{K}", "2") + ") UNION ALL "
                    + "SELECT 3 id, tid, rid FROM (SELECT t.id tid, r.id rid FROM " + body.replace("{K}", "3") + ")"
                    + ") ORDER BY 1, 2, 3";
            final String header = "id[INTEGER],tid[INTEGER],rid[INTEGER]\n";
            try (
                    PreparedStatement statement = connection.prepareStatement(sql);
                    PreparedStatement referenceStatement = connection.prepareStatement(reference)
            ) {
                final Integer[] firstValues = {null, null, 10, 10};
                final Long[] secondValues = {10L, null, null, 10L};
                final String[] rows = {"2,11,101\n", null, "", "1,10,100\n"};
                for (int i = 0; i < rows.length; i++) {
                    bind(statement, 1, firstValues[i], secondValues[i]);
                    if (rows[i] == null) {
                        assertNullValueError(statement, sql.indexOf('?') + 1);
                        continue;
                    }
                    try (ResultSet rs = statement.executeQuery()) {
                        assertResultSet(header + rows[i], sink, rs);
                    }
                    for (int k = 0; k < 3; k++) {
                        bind(referenceStatement, 2 * k + 1, firstValues[i], secondValues[i]);
                    }
                    try (ResultSet rs = referenceStatement.executeQuery()) {
                        assertResultSet(header + rows[i], sink, rs);
                    }
                }
            }
        });
    }

    // A prepared statement whose LATERAL body equates a.x to the outer key and compares a.id with it
    // too. Per outer row, a.id > o.x + 0 keeps a row 10 of o.x = 1 and a row 11 of o.x = 3, and a
    // NULL value makes the comparison false for every row. Each value runs twice.
    @Test
    public void testLateralWhereKeyAndCorrelatedComparisonPrepared() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE o (id INT, x INT)");
                statement.execute("CREATE TABLE a (id INT, x INT)");
                statement.execute("INSERT INTO o VALUES (1, 1), (2, NULL), (3, 3)");
                statement.execute("INSERT INTO a VALUES (10, 1), (11, 3), (12, NULL), (0, 1)");
            }
            final String sql = "SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.x = o.x AND a.id > o.x + ?) l ORDER BY 1, 2";
            final String header = "id[INTEGER],aid[INTEGER]\n";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (int i = 0; i < 2; i++) {
                    statement.setInt(1, 0);
                    try (ResultSet rs = statement.executeQuery()) {
                        assertResultSet(header + "1,10\n3,11\n", sink, rs);
                    }
                }
                for (int i = 0; i < 2; i++) {
                    statement.setNull(1, Types.INTEGER);
                    try (ResultSet rs = statement.executeQuery()) {
                        assertResultSet(header, sink, rs);
                    }
                }
            }
        });
    }

    // Executes the statement, which must fail with the NULL value error at the 1-based position
    private static void assertNullValueError(PreparedStatement statement, int position) throws SQLException {
        try (ResultSet ignore = statement.executeQuery()) {
            Assert.fail("expected the NULL value error");
        } catch (PSQLException e) {
            Assert.assertNotNull(e.getServerErrorMessage());
            Assert.assertEquals(LateralNullCheckRecordCursorFactory.NULL_VALUE_ERROR, e.getServerErrorMessage().getMessage());
            Assert.assertEquals(position, e.getServerErrorMessage().getPosition());
        }
    }

    private static void bind(PreparedStatement statement, int index, Integer first, Long second) throws SQLException {
        if (first == null) {
            statement.setNull(index, Types.INTEGER);
        } else {
            statement.setInt(index, first);
        }
        if (second == null) {
            statement.setNull(index + 1, Types.BIGINT);
        } else {
            statement.setLong(index + 1, second);
        }
    }

    // Three orders with the keys 1 to 3, and three refunds, one per key
    private static void createOrdersAndRefunds(Statement statement) throws SQLException {
        statement.execute("CREATE TABLE orders (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
        statement.execute("INSERT INTO orders VALUES (1, 1, 1::timestamp), (2, 2, 2::timestamp), (3, 3, 3::timestamp)");
        statement.execute("CREATE TABLE refunds (id INT, k INT, ts TIMESTAMP) TIMESTAMP(ts)");
        statement.execute("INSERT INTO refunds VALUES (100, 1, 1::timestamp), (101, 2, 2::timestamp), (102, 3, 3::timestamp)");
    }
}
