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

import io.questdb.std.str.StringSink;
import io.questdb.test.cutlass.suspend.TestCase;
import io.questdb.test.cutlass.suspend.TestCases;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;


/**
 * Cold storage may lead to the initiation of suspend events when data is inaccessible to the local database instance.
 * This disruption affects both the state machine's flow and the factory's data provision process. This test
 * replicates a suspend event, comparing the query output after resumption with the output of a query that
 * hasn't been suspended.
 */
@SuppressWarnings("SqlNoDataSourceInspection")
@RunWith(Parameterized.class)
public class PGQuerySuspendTest extends BasePGTest {

    private static final StringSink countSink = new StringSink();
    private static final StringSink sinkB = new StringSink();
    private final static TestCases testCases = new TestCases();

    public PGQuerySuspendTest(LegacyMode legacyMode) {
        super(legacyMode);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return legacyModeParams();
    }

    @Test
    public void testAllCases() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {

            // clear listeners - we do not want to emit suspend events in DDL statements
            // since this is not yet supported
            engine.releaseAllReaders();
            engine.setReaderListener(null);

            CallableStatement stmt = connection.prepareCall(testCases.getDdlX());
            stmt.execute();
            stmt = connection.prepareCall(testCases.getDdlY());
            stmt.execute();

            for (int i = 0; i < testCases.size(); i++) {
                TestCase tc = testCases.getQuick(i);

                engine.releaseAllReaders();
                engine.setReaderListener(null);

                try (PreparedStatement statement = connection.prepareStatement(tc.getQuery())) {
                    sink.clear();
                    if (tc.getBindVariableValues() != null) {
                        for (int j = 0; j < tc.getBindVariableValues().length; j++) {
                            statement.setString(j + 1, tc.getBindVariableValues()[j]);
                        }
                    }
                    try (ResultSet rs = statement.executeQuery()) {
                        long rows = printToSink(sink, rs, null);
                        if (!tc.isAllowEmptyResultSet()) {
                            Assert.assertTrue("Query " + tc.getQuery() + " is expected to return non-empty result set", rows > 0);
                        }
                    }
                }

                String countQuery = "select count(*) from (" + tc.getQuery() + ")";
                try (PreparedStatement statement = connection.prepareStatement(countQuery)) {
                    countSink.clear();
                    if (tc.getBindVariableValues() != null) {
                        for (int j = 0; j < tc.getBindVariableValues().length; j++) {
                            statement.setString(j + 1, tc.getBindVariableValues()[j]);
                        }
                    }
                    try (ResultSet rs = statement.executeQuery()) {
                        printToSink(countSink, rs, null);
                    }
                }

                engine.releaseAllReaders();

                // Yes, this write is racy, but it's not an issue in the test scenario.
                engine.setReaderListener(testCases.getSuspendingListener());
                try (PreparedStatement statement = connection.prepareStatement(tc.getQuery())) {
                    sinkB.clear();
                    if (tc.getBindVariableValues() != null) {
                        for (int j = 0; j < tc.getBindVariableValues().length; j++) {
                            statement.setString(j + 1, tc.getBindVariableValues()[j]);
                        }
                    }
                    try (ResultSet rs = statement.executeQuery()) {
                        printToSink(sinkB, rs, null);
                    }
                }

                TestUtils.assertEquals(tc.getQuery(), sink, sinkB);

                try (PreparedStatement statement = connection.prepareStatement(countQuery)) {
                    sinkB.clear();
                    if (tc.getBindVariableValues() != null) {
                        for (int j = 0; j < tc.getBindVariableValues().length; j++) {
                            statement.setString(j + 1, tc.getBindVariableValues()[j]);
                        }
                    }
                    try (ResultSet rs = statement.executeQuery()) {
                        printToSink(sinkB, rs, null);
                    }
                }

                TestUtils.assertEquals(countQuery, countSink, sinkB);
            }
        });
    }
}
