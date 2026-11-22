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

package io.questdb.test.cairo.mv;

import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A materialized view refreshes on the engine's behalf, with no caller to report a failure to. A
 * value of a row that does not parse as a tick expression acts as a null value there, rather than
 * invalidating the view, while the query of the view keeps failing on it when a user runs it.
 */
public class MatViewTickExprTest extends AbstractCairoTest {

    @Test
    public void testRowValueDateVariableResolvesLikeLiteral() throws Exception {
        // A refresh resolves the date variables of a value of a row against the "now" of its
        // context, as it resolves those of a literal, not against the clock of the machine, so
        // that every refresh, and every replica, gets the same rows
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            execute("CREATE TABLE base (ts TIMESTAMP, s STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW literal_view AS (SELECT ts, count() c FROM base WHERE ts IN '$today' SAMPLE BY 1h)");
            execute("CREATE MATERIALIZED VIEW row_value_view AS (SELECT ts, count() c FROM base WHERE ts IN s SAMPLE BY 1h)");
            // one row on the day of the clock of the machine, and one on the first day of the epoch
            execute("""
                    INSERT INTO base VALUES
                    ('1970-01-01T01:00:00.000000Z', '$today'),
                    ('2024-01-02T08:00:00.000000Z', '$today')
                    """);
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_name, view_status FROM materialized_views() ORDER BY view_name")
                    .noLeakCheck()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            view_name\tview_status
                            literal_view\tvalid
                            row_value_view\tvalid
                            """);
            // whichever day "$today" is in a refresh, it is one of the two days
            assertQuery("SELECT count() FROM row_value_view")
                    .noLeakCheck()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            count
                            1
                            """);
            assertSqlCursors("literal_view", "row_value_view");
        });
    }

    @Test
    public void testRowValueThatDoesNotParseKeepsViewValid() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            execute("CREATE TABLE base (ts TIMESTAMP, s STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW in_view AS (SELECT ts, count() c FROM base WHERE ts IN s SAMPLE BY 1h)");
            execute("CREATE MATERIALIZED VIEW not_in_view AS (SELECT ts, count() c FROM base WHERE ts NOT IN s SAMPLE BY 1h)");
            execute("""
                    INSERT INTO base VALUES
                    ('2024-01-02T08:00:00.000000Z', '2024-01-02'),
                    ('2024-01-02T09:00:00.000000Z', 'garbage'),
                    ('2024-01-02T10:00:00.000000Z', '2024-01-02T10:00'),
                    ('2024-01-02T11:00:00.000000Z', '2024-01-03')
                    """);
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_name, view_status FROM materialized_views() ORDER BY view_name")
                    .noLeakCheck()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            view_name\tview_status
                            in_view\tvalid
                            not_in_view\tvalid
                            """);
            // the value that does not parse acts as null: no timestamp is in it
            assertQuery("in_view")
                    .noLeakCheck()
                    .inferRandomAccess()
                    .inferTimestamp()
                    .sizeMayVary()
                    .returns("""
                            ts\tc
                            2024-01-02T08:00:00.000000Z\t1
                            2024-01-02T10:00:00.000000Z\t1
                            """);
            assertQuery("not_in_view")
                    .noLeakCheck()
                    .inferRandomAccess()
                    .inferTimestamp()
                    .sizeMayVary()
                    .returns("""
                            ts\tc
                            2024-01-02T09:00:00.000000Z\t1
                            2024-01-02T11:00:00.000000Z\t1
                            """);
            // the query of the view fails on the value when a user runs it
            final String query = "SELECT ts, count() c FROM base WHERE ts IN s SAMPLE BY 1h";
            assertExceptionNoLeakCheck(query, query.indexOf(" s ") + 1, "Invalid date: garbage");
        });
    }
}
