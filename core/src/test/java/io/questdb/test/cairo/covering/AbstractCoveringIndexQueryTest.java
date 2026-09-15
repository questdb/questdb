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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;

public abstract class AbstractCoveringIndexQueryTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        super.setUp();
    }

    /**
     * Assert the indexed arm returns exactly what a full scan returns.
     * <p>
     * The reference arm MUST use {@code /*+ no_index *}{@code /}. Do not use
     * {@code /*+ no_covering *}{@code /}: it does not disable the index, it routes to
     * FilterOnValues plus a serial group by -- a third execution path, and the slowest
     * of the four. An arm mislabelled that way has already produced one wrong baseline.
     */
    protected void assertSameResult(String indexedSql, String referenceSql) throws Exception {
        final StringSink indexed = new StringSink();
        final StringSink reference = new StringSink();
        printSql(indexedSql, indexed);
        printSql(referenceSql, reference);
        TestUtils.assertEquals(reference, indexed);
    }

    protected void createFlights() throws Exception {
        execute("CREATE TABLE flights (flight_ground INT, start_time TIMESTAMP, end_time TIMESTAMP)");
        execute("INSERT INTO flights VALUES (100, '1970-01-01T00:00:02.000000Z', '1970-01-01T00:00:08.000000Z')");
    }

    protected void createTelemetry() throws Exception {
        createTelemetryTable();
        execute("INSERT INTO telemetry SELECT (x * 1000000L)::timestamp," +
                " CASE WHEN x % 4 = 0 THEN 'SFID' WHEN x % 4 = 1 THEN 'HOTMIC'" +
                "      WHEN x % 4 = 2 THEN 'KCAS' ELSE 'CALT' END," +
                " x::double" +
                " FROM long_sequence(10000)");
    }

    protected void createTelemetryWithNulls() throws Exception {
        createTelemetryTable();
        execute("INSERT INTO telemetry SELECT (x * 1000000L)::timestamp," +
                " CASE WHEN x % 4 = 0 THEN 'SFID' WHEN x % 4 = 1 THEN 'HOTMIC'" +
                "      WHEN x % 4 = 2 THEN 'KCAS' ELSE 'CALT' END," +
                " CASE WHEN x % 5 = 0 THEN NULL ELSE x::double END" +
                " FROM long_sequence(10000)");
    }

    private void createTelemetryTable() throws Exception {
        execute("CREATE TABLE telemetry (" +
                "  ts TIMESTAMP," +
                "  param_id SYMBOL INDEX TYPE POSTING INCLUDE (ts, value)," +
                "  value DOUBLE" +
                ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
    }
}
