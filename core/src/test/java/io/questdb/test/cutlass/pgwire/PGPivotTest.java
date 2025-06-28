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

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;


@SuppressWarnings("SqlNoDataSourceInspection")
@RunWith(Parameterized.class)
public class PGPivotTest extends BasePGTest {

    public PGPivotTest(@NonNull LegacyMode legacyMode) {
        super(legacyMode);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {LegacyMode.MODERN},
        });
    }

    @Test
    public void testDynamicPivotIsNotCached() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement ps = connection.prepareStatement("CREATE TABLE foo (x INT, y INT);")) {
                Assert.assertFalse(ps.execute());
            }

            try (PreparedStatement ps = connection.prepareStatement("INSERT INTO foo (x, y) VALUES (1, 5), (2, 6);")) {
                Assert.assertFalse(ps.execute());
            }

            try (PreparedStatement ps = connection.prepareStatement("foo PIVOT (sum(y) FOR x IN (SELECT DISTINCT x FROM foo ORDER BY x))")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            "1[BIGINT],2[BIGINT]\n" +
                                    "5,6\n",
                            sink,
                            rs
                    );
                }
            }

            try (PreparedStatement ps = connection.prepareStatement("INSERT INTO foo (x, y) VALUES (3, 7), (4, 8);")) {
                Assert.assertFalse(ps.execute());
            }

            try (PreparedStatement ps = connection.prepareStatement("foo PIVOT (sum(y) FOR x IN (SELECT DISTINCT x FROM foo ORDER BY x))")) {

                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            "1[BIGINT],2[BIGINT],3[BIGINT],4[BIGINT]\n" +
                                    "5,6,7,8\n",
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testDynamicPivotWithBindVariables() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement ps = connection.prepareStatement("CREATE TABLE foo (x INT, y INT);")) {
                Assert.assertFalse(ps.execute());
            }

            try (PreparedStatement ps = connection.prepareStatement("INSERT INTO foo (x, y) VALUES (1, 5), (2, 6);")) {
                Assert.assertFalse(ps.execute());
            }

            try (PreparedStatement ps = connection.prepareStatement("foo PIVOT (sum(y) FOR x IN (SELECT DISTINCT x FROM foo WHERE x IN ? ORDER BY x))")) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            "1[BIGINT]\n" +
                                    "5\n",
                            sink,
                            rs
                    );
                    if (mode != Mode.SIMPLE) {
                        Assert.fail("should not be supported");
                    }
                }
                catch (SQLException e) {
                    Assert.assertEquals("ERROR: cannot use bind variables in a PIVOT query\n" +
                            "  Position: 80", e.getMessage());
                }
            }
        });
    }


    @Test
    public void testBindVariablesAreAllowedElsewhereInPivot() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement ps = connection.prepareStatement("CREATE TABLE foo (x INT, y INT);")) {
                Assert.assertFalse(ps.execute());
            }

            try (PreparedStatement ps = connection.prepareStatement("INSERT INTO foo (x, y) VALUES (1, 5), (2, 6);")) {
                Assert.assertFalse(ps.execute());
            }

            try (PreparedStatement ps = connection.prepareStatement("foo PIVOT (sum(y) FOR x IN (SELECT DISTINCT x FROM foo ORDER BY x) GROUP BY x) LIMIT ?")) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            "x[INTEGER],1[BIGINT],2[BIGINT]\n" +
                                    "1,5,null\n",
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testRegularPivotWithBindVariables() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement ps = connection.prepareStatement("CREATE TABLE foo (x INT, y INT);")) {
                Assert.assertFalse(ps.execute());
            }

            try (PreparedStatement ps = connection.prepareStatement("INSERT INTO foo (x, y) VALUES (1, 5), (2, 6);")) {
                Assert.assertFalse(ps.execute());
            }

            try (PreparedStatement ps = connection.prepareStatement("foo PIVOT (sum(y) FOR x IN (?))")) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            "1[BIGINT]\n" +
                                    "5\n",
                            sink,
                            rs
                    );
                    if (mode != Mode.SIMPLE) {
                        Assert.fail("should not be supported");
                    }
                }
                catch (SQLException e) {
                    Assert.assertEquals("ERROR: literal IN list must must only contain constants\n" +
                            "  Position: 31",e.getMessage());
                }
            }
        });
    }

    @Test
    public void testPivotWithPortal() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            try (PreparedStatement ps = connection.prepareStatement("CREATE TABLE foo (x INT, y INT, z INT);")) {
                Assert.assertFalse(ps.execute());
            }

            try (PreparedStatement ps = connection.prepareStatement("INSERT INTO foo (x, y, z) VALUES (1, 2, 3), (4,5,6), (7,8,9), (10,11,12), (13,14,15), (16,17,18), (19,20,21), (21,22,23), (24,25,26), (27, 28, 29), (30, 31, 32), (33, 34, 35), (36, 37, 38), (39, 40, 41);")) {
                Assert.assertFalse(ps.execute());
            }

            connection.commit();

            try (PreparedStatement ps = connection.prepareStatement("foo PIVOT (sum(y) FOR x IN (SELECT DISTINCT x FROM foo ORDER BY x) GROUP BY z)")) {
                ps.setFetchSize(1);
                Assert.assertTrue(ps.execute());

                try (ResultSet rs = ps.executeQuery()) {
                    rs.setFetchSize(1);
                    int count = 0;

                    while (rs.next()) {
                        ++count;
                    }

                    Assert.assertEquals(14, count);
                }
            }
        });
    }
}


