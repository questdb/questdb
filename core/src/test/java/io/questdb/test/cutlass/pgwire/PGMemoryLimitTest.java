/*******************************************************************************
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

import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static io.questdb.test.tools.TestUtils.assertContains;

public class PGMemoryLimitTest extends BasePGTest {
    @Test
    public void testUpdateRecoversFromOomError() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement stat = connection.createStatement()) {
                stat.execute(
                        "create table up as" +
                                " (select timestamp_sequence(0, 1000000) ts," +
                                " x" +
                                " from long_sequence(5))" +
                                " timestamp(ts) partition by DAY;"
                );
                stat.execute(
                        "create table down as" +
                                " (select timestamp_sequence(0, 1000000) ts," +
                                " x * 100 as y" +
                                " from long_sequence(5))" +
                                " timestamp(ts) partition by DAY;"
                );
            }

            try (Statement stat = connection.createStatement()) {
                // Set RSS limit, so that the UPDATE will fail with OOM.
                Unsafe.setRssMemLimit(8_900_000);
                try {
                    stat.execute(
                            "UPDATE up SET x = y" +
                                    " FROM down " +
                                    " WHERE up.ts = down.ts and x < 4"
                    );
                    Assert.fail();
                } catch (PSQLException e) {
                    assertContains(e.getMessage(), "global RSS memory limit exceeded");
                }

                // Remove the limit and verify that the update succeeds.
                Unsafe.setRssMemLimit(0);
                stat.execute(
                        "UPDATE up SET x = y" +
                                " FROM down " +
                                " WHERE up.ts = down.ts and x < 4"
                );
                drainWalQueue();
            }
            final String expected = "ts[TIMESTAMP],x[BIGINT]\n" +
                    "1970-01-01 00:00:00.0,100\n" +
                    "1970-01-01 00:00:01.0,200\n" +
                    "1970-01-01 00:00:02.0,300\n" +
                    "1970-01-01 00:00:03.0,4\n" +
                    "1970-01-01 00:00:04.0,5\n";
            try (ResultSet resultSet = connection.prepareStatement("up").executeQuery()) {
                sink.clear();
                assertResultSet(expected, sink, resultSet);
            }
        });
    }

    @Test
    public void testUpdateRecoversFromOomErrorBindVars() throws Exception {
        // the reason "simple" is excluded is dues to PG driver sending
        // update x = '20', where x is long and '20' is, well, string. We don't support such conversion at
        // the update level yet
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            try (Statement stat = connection.createStatement()) {
                stat.execute(
                        "create table up as" +
                                " (select timestamp_sequence(0, 1000000) ts," +
                                " x" +
                                " from long_sequence(5))" +
                                " timestamp(ts) partition by DAY;"
                );
                stat.execute(
                        "create table down as" +
                                " (select timestamp_sequence(0, 1000000) ts," +
                                " x * 100 as y" +
                                " from long_sequence(5))" +
                                " timestamp(ts) partition by DAY;"
                );
            }

            Unsafe.setRssMemLimit(8_900_000);
            try (
                    PreparedStatement ps = connection.prepareStatement("UPDATE up SET x = ?" +
                            " FROM down " +
                            " WHERE up.ts = down.ts and x < 4")
            ) {
                // Set RSS limit, so that the UPDATE will fail with OOM.
                try {
                    ps.setInt(1, 10);
                    ps.execute();
                    Assert.fail();
                } catch (PSQLException e) {
                    assertContains(e.getMessage(), "global RSS memory limit exceeded");
                }

                // Remove the limit and verify that the update succeeds.
                Unsafe.setRssMemLimit(0);
                ps.setInt(1, 20);
                ps.execute();
            }

            final String expected = "ts[TIMESTAMP],x[BIGINT]\n" +
                    "1970-01-01 00:00:00.0,20\n" +
                    "1970-01-01 00:00:01.0,20\n" +
                    "1970-01-01 00:00:02.0,20\n" +
                    "1970-01-01 00:00:03.0,4\n" +
                    "1970-01-01 00:00:04.0,5\n";
            try (ResultSet resultSet = connection.prepareStatement("up").executeQuery()) {
                sink.clear();
                assertResultSet(expected, sink, resultSet);
            }
        });
    }
}
