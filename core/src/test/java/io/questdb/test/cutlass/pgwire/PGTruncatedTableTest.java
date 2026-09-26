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

import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PGTruncatedTableTest extends BasePGTest {

    @Test
    public void testPreparedStatementsFollowTruncate() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
            execute("CREATE TABLE t (g SYMBOL INDEX, status SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('aa', 'target', 10, '2024-01-01T00:00:01Z'),
                    ('bb', 'other', 20, '2024-01-01T00:00:02Z')
                    """);
            try (
                    PreparedStatement filtered = connection.prepareStatement(
                            "SELECT g, v FROM t WHERE status = 'target' LATEST ON ts PARTITION BY g"
                    );
                    PreparedStatement indexed = connection.prepareStatement(
                            "SELECT g, v FROM t WHERE g = 'aa' OR g = 'bb' LATEST ON ts PARTITION BY g"
                    );
                    PreparedStatement indexedKey = connection.prepareStatement("SELECT g, v FROM t WHERE g = 'aa'");
                    PreparedStatement indexedList = connection.prepareStatement("SELECT g, v FROM t WHERE g IN ('aa', 'bb')");
                    PreparedStatement jitFiltered = connection.prepareStatement("SELECT g, v FROM t WHERE status = 'target'")
            ) {
                assertQueryResult(filtered, "g[VARCHAR],v[BIGINT]\naa,10\n");
                assertQueryResult(indexed, "g[VARCHAR],v[BIGINT]\naa,10\nbb,20\n");
                assertQueryResult(indexedKey, "g[VARCHAR],v[BIGINT]\naa,10\n");
                assertQueryResult(indexedList, "g[VARCHAR],v[BIGINT]\naa,10\nbb,20\n");
                assertQueryResult(jitFiltered, "g[VARCHAR],v[BIGINT]\naa,10\n");
                execute("TRUNCATE TABLE t");
                execute("""
                        INSERT INTO t VALUES
                        ('cc', 'other', 30, '2024-01-01T00:00:03Z'),
                        ('dd', 'other', 40, '2024-01-01T00:00:04Z'),
                        ('aa', 'target', 50, '2024-01-01T00:00:05Z'),
                        ('bb', 'target', 60, '2024-01-01T00:00:06Z')
                        """);
                assertQueryResult(filtered, "g[VARCHAR],v[BIGINT]\naa,50\nbb,60\n");
                assertQueryResult(indexed, "g[VARCHAR],v[BIGINT]\naa,50\nbb,60\n");
                assertQueryResult(indexedKey, "g[VARCHAR],v[BIGINT]\naa,50\n");
                assertQueryResult(indexedList, "g[VARCHAR],v[BIGINT]\naa,50\nbb,60\n");
                assertQueryResult(jitFiltered, "g[VARCHAR],v[BIGINT]\naa,50\nbb,60\n");
            }
        });
    }

    private void assertQueryResult(PreparedStatement statement, String expected) throws Exception {
        sink.clear();
        try (ResultSet rs = statement.executeQuery()) {
            assertResultSet(expected, sink, rs);
        }
    }
}
