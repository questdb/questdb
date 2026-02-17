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

package io.questdb.test.cairo.view;

import io.questdb.PropertyKey;
import org.junit.Test;

public class AlterViewTest extends AbstractViewTest {

    @Test
    public void testAlterView() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 5";
            createView(VIEW1, query1, TABLE1);

            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    VIEW1,
                    """
                            QUERY PLAN
                            Async Group By workers: 1
                              keys: [ts,k]
                              values: [max(v)]
                              filter: 5<v
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            assertViewMetadata("{" +
                    "\"columnCount\":3," +
                    "\"columns\":[" +
                    "{\"index\":0,\"name\":\"ts\",\"type\":\"TIMESTAMP\"}," +
                    "{\"index\":1,\"name\":\"k\",\"type\":\"SYMBOL\"}," +
                    "{\"index\":2,\"name\":\"v_max\",\"type\":\"LONG\"}" +
                    "]," +
                    "\"timestampIndex\":-1" +
                    "}");

            final String query2 = "select ts, k, min(v) as v_min from " + TABLE1 + " where v > 6";
            alterView(query2, TABLE1);

            assertQueryAndPlan(
                    """
                            ts\tk\tv_min
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    VIEW1,
                    """
                            QUERY PLAN
                            Async Group By workers: 1
                              keys: [ts,k]
                              values: [min(v)]
                              filter: 6<v
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            assertViewMetadata("{" +
                    "\"columnCount\":3," +
                    "\"columns\":[" +
                    "{\"index\":0,\"name\":\"ts\",\"type\":\"TIMESTAMP\"}," +
                    "{\"index\":1,\"name\":\"k\",\"type\":\"SYMBOL\"}," +
                    "{\"index\":2,\"name\":\"v_min\",\"type\":\"LONG\"}" +
                    "]," +
                    "\"timestampIndex\":-1" +
                    "}");

            final String query3 = "select t1.ts, t2.v from " + TABLE1 + " t1 join " + TABLE2 + " t2 on k where t2.ts > '1970-01-01T00:01:00'";
            alterView(query3, TABLE1, TABLE2);

            assertQueryAndPlan(
                    """
                            ts\tv
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    VIEW1, "ts", false, false,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Hash Join Light
                                  condition: t2.k=t1.k
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Interval forward scan on: table2
                                              intervals: [("1970-01-01T00:01:00.000001Z","MAX")]
                            """,
                    VIEW1
            );

            assertViewMetadata("{" +
                    "\"columnCount\":2," +
                    "\"columns\":[" +
                    "{\"index\":0,\"name\":\"ts\",\"type\":\"TIMESTAMP\"}," +
                    "{\"index\":1,\"name\":\"v\",\"type\":\"LONG\"}" +
                    "]," +
                    "\"timestampIndex\":0" +
                    "}");
        });
    }

    @Test
    public void testAlterViewRejectsInvalidColumnName() throws Exception {
        // explicitly enable expression-based column names to test that view alteration
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");

        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 5";
            createView(VIEW1, query1, TABLE1);

            drainWalAndViewQueues();

            // when altering the view then column names must still be valid - the same as for table creation
            assertExceptionNoLeakCheck(
                    "CREATE OR REPLACE VIEW " + VIEW1 + " AS (SELECT v * 10 FROM " + TABLE1 + ")",
                    32,
                    "invalid column name [name=v * 10"
            );
        });
    }

    @Test
    public void testCreateOrReplaceView() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 5";
            createOrReplaceView(query1, TABLE1);

            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    VIEW1,
                    """
                            QUERY PLAN
                            Async Group By workers: 1
                              keys: [ts,k]
                              values: [max(v)]
                              filter: 5<v
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            assertViewMetadata("{" +
                    "\"columnCount\":3," +
                    "\"columns\":[" +
                    "{\"index\":0,\"name\":\"ts\",\"type\":\"TIMESTAMP\"}," +
                    "{\"index\":1,\"name\":\"k\",\"type\":\"SYMBOL\"}," +
                    "{\"index\":2,\"name\":\"v_max\",\"type\":\"LONG\"}" +
                    "]," +
                    "\"timestampIndex\":-1" +
                    "}");

            final String query2 = "select ts, k, min(v) as v_min from " + TABLE1 + " where v > 6";
            createOrReplaceView(query2, TABLE1);

            assertQueryAndPlan(
                    """
                            ts\tk\tv_min
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    VIEW1,
                    """
                            QUERY PLAN
                            Async Group By workers: 1
                              keys: [ts,k]
                              values: [min(v)]
                              filter: 6<v
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            assertViewMetadata("{" +
                    "\"columnCount\":3," +
                    "\"columns\":[" +
                    "{\"index\":0,\"name\":\"ts\",\"type\":\"TIMESTAMP\"}," +
                    "{\"index\":1,\"name\":\"k\",\"type\":\"SYMBOL\"}," +
                    "{\"index\":2,\"name\":\"v_min\",\"type\":\"LONG\"}" +
                    "]," +
                    "\"timestampIndex\":-1" +
                    "}");

            final String query3 = "select t1.ts, t2.v from " + TABLE1 + " t1 join " + TABLE2 + " t2 on k where t2.ts > '1970-01-01T00:01:00'";
            createOrReplaceView(query3, TABLE1, TABLE2);

            assertQueryAndPlan(
                    """
                            ts\tv
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    VIEW1, "ts", false, false,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Hash Join Light
                                  condition: t2.k=t1.k
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Interval forward scan on: table2
                                              intervals: [("1970-01-01T00:01:00.000001Z","MAX")]
                            """,
                    VIEW1
            );

            assertViewMetadata("{" +
                    "\"columnCount\":2," +
                    "\"columns\":[" +
                    "{\"index\":0,\"name\":\"ts\",\"type\":\"TIMESTAMP\"}," +
                    "{\"index\":1,\"name\":\"v\",\"type\":\"LONG\"}" +
                    "]," +
                    "\"timestampIndex\":0" +
                    "}");
        });
    }
}
