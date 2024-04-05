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

package io.questdb.test;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.std.str.StringSink;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static io.questdb.test.cutlass.pgwire.BasePGTest.assertResultSet;
import static io.questdb.test.tools.TestUtils.unchecked;

public class ServerMainQuerySmokeTest extends AbstractBootstrapTest {
    private static final StringSink sink = new StringSink();

    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration(
                // Force enable parallel GROUP BY and filter for smoke tests.
                PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED + "=true",
                PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED.getPropertyPath() + "=true"
        ));
        dbPath.parent().$();
    }

    @Test
    public void testServerMainAsyncFilterSmokeTest() throws Exception {
        // Verify that circuit breaker checks don't have weird bugs unseen in fast tests.
        try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();
            try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                try (Statement statement = conn.createStatement()) {
                    statement.execute("create table x as (select x % 10000 l from long_sequence(1000000));");
                }

                String query = "select count() from (select * from x where l = 42);";
                String expected = "count[BIGINT]\n" +
                        "100\n";
                try (ResultSet rs = conn.prepareStatement(query).executeQuery()) {
                    sink.clear();
                    assertResultSet(expected, sink, rs);
                }
            }
        }
    }

    @Test
    public void testServerMainAsyncGroupBySmokeTest1() throws Exception {
        // Verify that circuit breaker checks don't have weird bugs unseen in fast tests.
        try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();
            try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                try (Statement statement = conn.createStatement()) {
                    statement.execute("create table x as (select x % 10000 l1, x % 1000 l2 from long_sequence(1000000));");
                }

                String query = "select count_distinct(l1), count_distinct(l2) from x;";
                String expected = "count_distinct[BIGINT],count_distinct1[BIGINT]\n" +
                        "10000,1000\n";
                try (ResultSet rs = conn.prepareStatement(query).executeQuery()) {
                    sink.clear();
                    assertResultSet(expected, sink, rs);
                }
            }
        }
    }

    @Test
    public void testServerMainAsyncGroupBySmokeTest2() throws Exception {
        // Verify that circuit breaker checks don't have weird bugs unseen in fast tests.
        try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();
            try (Connection conn = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                try (Statement statement = conn.createStatement()) {
                    statement.execute("create table x as (select 'k' || (x % 3) k, x % 10000 l from long_sequence(1000000));");
                }

                String query = "select k, count_distinct(l) from x order by k;";
                String expected = "k[VARCHAR],count_distinct[BIGINT]\n" +
                        "k0,10000\n" +
                        "k1,10000\n" +
                        "k2,10000\n";
                try (ResultSet rs = conn.prepareStatement(query).executeQuery()) {
                    sink.clear();
                    assertResultSet(expected, sink, rs);
                }
            }
        }
    }
}
