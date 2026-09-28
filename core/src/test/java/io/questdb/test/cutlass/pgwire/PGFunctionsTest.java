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

import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import static io.questdb.cairo.sql.SqlExecutionCircuitBreaker.TIMEOUT_FAIL_ON_FIRST_CHECK;

public class PGFunctionsTest extends BasePGTest {

    @Test
    public void testListTablesDoesntLeakMetaFds() throws Exception {
        maxQueryTime = TIMEOUT_FAIL_ON_FIRST_CHECK;
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            try (CallableStatement st1 = connection.prepareCall("create table a (i int)")) {
                st1.execute();
            }
            sink.clear();
            long openFilesBefore = TestFilesFacadeImpl.INSTANCE.getOpenFileCount();
            // tables() honors the circuit breaker, so with a breaker that trips on the first check
            // the listing is aborted before it acquires any metadata. It must not leak metadata FDs.
            try (PreparedStatement ps = connection.prepareStatement("select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables()")) {
                ps.executeQuery();
                Assert.fail("expected the query to be aborted by the circuit breaker");
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "timeout, query aborted");
            }
            engine.releaseAllReaders();
            long openFilesAfter = TestFilesFacadeImpl.INSTANCE.getOpenFileCount();
            Assert.assertEquals(openFilesBefore, openFilesAfter);
        });
    }

    @Test
    public void testLongSequenceBindVariables() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            try (PreparedStatement ps = connection.prepareStatement("SELECT x FROM long_sequence(?)")) {
                ps.setLong(1, 2);
                assertQueryRows("x[BIGINT]\n1\n2\n", ps);
                ps.setLong(1, 3);
                assertQueryRows("x[BIGINT]\n1\n2\n3\n", ps);
                // node-postgres and other drivers send untyped parameters
                ps.setObject(1, "2", Types.OTHER);
                assertQueryRows("x[BIGINT]\n1\n2\n", ps);

                ps.setDouble(1, 2.0);
                try (ResultSet ignore = ps.executeQuery()) {
                    Assert.fail("a DOUBLE count must be rejected");
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "argument type DOUBLE is not supported");
                }
            }

            final String expected;
            try (PreparedStatement ps = connection.prepareStatement("SELECT x, rnd_long() r FROM long_sequence(3, 1, 2)")) {
                try (ResultSet rs = ps.executeQuery()) {
                    sink.clear();
                    printToSink(sink, rs, null);
                    expected = sink.toString();
                }
            }
            try (PreparedStatement ps = connection.prepareStatement("SELECT x, rnd_long() r FROM long_sequence(?, ?, ?)")) {
                ps.setObject(1, "3", Types.OTHER);
                ps.setObject(2, "1", Types.OTHER);
                ps.setObject(3, "2", Types.OTHER);
                assertQueryRows(expected, ps);
                ps.setLong(1, 3);
                ps.setLong(2, 1);
                ps.setLong(3, 2);
                assertQueryRows(expected, ps);
            }
        });
    }

    private static void assertQueryRows(String expected, PreparedStatement ps) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            sink.clear();
            assertResultSet(expected, sink, rs);
        }
    }
}
