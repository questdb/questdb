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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.test.cairo.TableModel;
import org.junit.Test;

public class SampleBySqlParserTest extends AbstractSqlParserTest {
    private static final String DDL = "CREATE TABLE x (a DOUBLE, b SYMBOL, k TIMESTAMP, timestamp TIMESTAMP) TIMESTAMP(timestamp)";

    @Test
    public void testAlignExpected() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h hello",
                48,
                "unexpected token [hello]",
                model()
        );
    }

    @Test
    public void testAlignToCalendarFollowedByInvalid() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar blah",
                66,
                "unexpected token [blah]",
                model()
        );
    }

    @Test
    public void testAlignToCalendarNonConstantTimeZone() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone rnd_str('foo','bar') with offset '00:15'",
                76,
                "timezone must be a constant expression of STRING or CHAR type",
                model()
        );
    }

    @Test
    public void testAlignToCalendarTimeMissingZone() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time",
                70,
                "'zone' expected",
                model()
        );
    }

    @Test
    public void testAlignToCalendarTimeZoneExpected() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time lunch",
                71,
                "'zone' expected",
                model()
        );
    }

    @Test
    public void testAlignToCalendarTimeZoneFollowedByUnexpectedToken() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone 'X' zone",
                80,
                "unexpected token [zone]",
                model()
        );
    }

    @Test
    public void testAlignToCalendarTimeZoneMissingZoneName() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone",
                75,
                "Expression expected",
                model()
        );
    }

    @Test
    public void testAlignToCalendarTimeZoneWithMissingOffset() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone 'X' with",
                84,
                "'offset' expected",
                model()
        );
    }

    @Test
    public void testAlignToCalendarTimeZoneWithNonConstantOffset() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone 'X' with offset rnd_str('foo','bar')",
                76,
                "invalid timezone: X",
                model()
        );
    }

    @Test
    public void testAlignToCalendarTimeZoneWithNonStringConstantOffset() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone 'X' with offset '00:01'",
                76,
                "invalid timezone: X",
                model()
        );
    }

    @Test
    public void testAlignToCalendarTimeZoneWithOffsetMissingExpression() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone 'X' with offset",
                91,
                "Expression expected",
                model()
        );
    }

    @Test
    public void testAlignToCalendarTimeZoneWithSomethingUnexpected() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone 'X' with friends",
                85,
                "'offset' expected",
                model()
        );
    }

    @Test
    public void testAlignToCalendarWithTimeZoneAndLimit() throws Exception {
        assertQuery("select a, sum(a) from x sample by 1h align to calendar time zone 'UTC' limit 1;")
                .ddl(DDL)
                .assertsLogicalPlan("""
                        Limit
                          lo: 1
                          Project
                            columns: [a, sum]
                            Sort
                              keys: [timestamp]
                              Aggregate
                                keys: [a, timestamp_floor_utc('1h', timestamp, null, '00:00', null) AS timestamp]
                                values: [sum(a) AS sum]
                                Scan
                                  table: x
                                  columns: [a, timestamp]
                        """);
    }

    @Test
    public void testAlignToCalendarWithTimeZoneAndOrderBy() throws Exception {
        assertQuery("select a, sum(a) from x sample by 1h align to calendar time zone 'UTC' order by a desc;")
                .ddl(DDL)
                .assertsLogicalPlan("""
                        Sort
                          keys: [a desc]
                          Project
                            columns: [a, sum]
                            Aggregate
                              keys: [a, timestamp_floor_utc('1h', timestamp, null, '00:00', null) AS timestamp]
                              values: [sum(a) AS sum]
                              Scan
                                table: x
                                columns: [a, timestamp]
                        """);
    }

    @Test
    public void testAlignToCalendarWithTimeZoneEndingWithSemicolon() throws Exception {
        assertQuery("select a, sum(a) from x sample by 1h align to calendar time zone 'UTC';")
                .ddl(DDL)
                .assertsLogicalPlan("""
                        Project
                          columns: [a, sum]
                          Sort
                            keys: [timestamp]
                            Aggregate
                              keys: [a, timestamp_floor_utc('1h', timestamp, null, '00:00', null) AS timestamp]
                              values: [sum(a) AS sum]
                              Scan
                                table: x
                                columns: [a, timestamp]
                        """);
    }

    @Test
    public void testAlignToCalendarWithoutTimezoneNorOffsetAndLimit() throws Exception {
        assertQuery("select a, sum(a) from x sample by 1h align to calendar limit 1;")
                .ddl(DDL)
                .assertsLogicalPlan("""
                        Limit
                          lo: 1
                          Project
                            columns: [a, sum]
                            Sort
                              keys: [timestamp]
                              Aggregate
                                keys: [a, timestamp_floor_utc('1h', timestamp, null, '00:00', null) AS timestamp]
                                values: [sum(a) AS sum]
                                Scan
                                  table: x
                                  columns: [a, timestamp]
                        """);
    }

    @Test
    public void testAlignToSomethingInvalid() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to there",
                57,
                "'calendar' or 'first observation' expected",
                model()
        );
    }

    @Test
    public void testCalendar() throws Exception {
        assertQuery("select b, sum(a), k k1, k from x y sample by 3h align to calendar")
                .ddl(DDL)
                .assertsLogicalPlan("""
                        Project
                          columns: [b, sum, k1, k]
                          Sort
                            keys: [timestamp]
                            Project
                              columns: [b, sum, k1, k1 AS k, timestamp]
                              Aggregate
                                keys: [b, k AS k1, timestamp_floor_utc('3h', timestamp, null, '00:00', null) AS timestamp]
                                values: [sum(a) AS sum]
                                Scan
                                  table: x
                                  columns: [a, b, k, timestamp]
                        """);
    }

    @Test
    public void testCalendarTimeZone() throws Exception {
        assertQuery("select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone 'CET'")
                .ddl(DDL)
                .assertsLogicalPlan("""
                        Project
                          columns: [b, sum, k1, k]
                          Sort
                            keys: [timestamp]
                            Project
                              columns: [b, sum, k1, k1 AS k, timestamp]
                              Aggregate
                                keys: [b, k AS k1, timestamp_floor_utc('3h', timestamp, null, '00:00', 'CET') AS timestamp]
                                values: [sum(a) AS sum]
                                Scan
                                  table: x
                                  columns: [a, b, k, timestamp]
                        """);
    }

    @Test
    public void testCalendarTimeZoneAndOffsetAsBindVariables() throws Exception {
        assertQuery("select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone $1 with offset $2")
                .ddl(DDL)
                .assertsLogicalPlan("""
                        Project
                          columns: [b, sum, k1, k]
                          Sort
                            keys: [timestamp]
                            Project
                              columns: [b, sum, k1, k1 AS k, timestamp]
                              Aggregate
                                keys: [b, k AS k1, timestamp_floor_utc('3h', timestamp, null, $2, $1) AS timestamp]
                                values: [sum(a) AS sum]
                                Scan
                                  table: x
                                  columns: [a, b, k, timestamp]
                        """);
    }

    @Test
    public void testCalendarTimeZoneAsOffset() throws Exception {
        assertQuery("select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone '+01:00'")
                .ddl(DDL)
                .assertsLogicalPlan("""
                        Project
                          columns: [b, sum, k1, k]
                          Sort
                            keys: [timestamp]
                            Project
                              columns: [b, sum, k1, k1 AS k, timestamp]
                              Aggregate
                                keys: [b, k AS k1, timestamp_floor_utc('3h', timestamp, null, '00:00', '+01:00') AS timestamp]
                                values: [sum(a) AS sum]
                                Scan
                                  table: x
                                  columns: [a, b, k, timestamp]
                        """);
    }

    @Test
    public void testCalendarTimeZoneAsOffsetNegative() throws Exception {
        assertQuery("select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone '-04:00'")
                .ddl(DDL)
                .assertsLogicalPlan("""
                        Project
                          columns: [b, sum, k1, k]
                          Sort
                            keys: [timestamp]
                            Project
                              columns: [b, sum, k1, k1 AS k, timestamp]
                              Aggregate
                                keys: [b, k AS k1, timestamp_floor_utc('3h', timestamp, null, '00:00', '-04:00') AS timestamp]
                                values: [sum(a) AS sum]
                                Scan
                                  table: x
                                  columns: [a, b, k, timestamp]
                        """);
    }

    @Test
    public void testCalendarTimeZoneWithOffsetNegative() throws Exception {
        assertQuery("select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone 'CET' with offset '-00:15'")
                .ddl(DDL)
                .assertsLogicalPlan("""
                        Project
                          columns: [b, sum, k1, k]
                          Sort
                            keys: [timestamp]
                            Project
                              columns: [b, sum, k1, k1 AS k, timestamp]
                              Aggregate
                                keys: [b, k AS k1, timestamp_floor_utc('3h', timestamp, null, '-00:15', 'CET') AS timestamp]
                                values: [sum(a) AS sum]
                                Scan
                                  table: x
                                  columns: [a, b, k, timestamp]
                        """);
    }

    @Test
    public void testCalendarTimeZoneWithOffsetPositive() throws Exception {
        assertQuery("select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone 'CET' with offset '00:15'")
                .ddl(DDL)
                .assertsLogicalPlan("""
                        Project
                          columns: [b, sum, k1, k]
                          Sort
                            keys: [timestamp]
                            Project
                              columns: [b, sum, k1, k1 AS k, timestamp]
                              Aggregate
                                keys: [b, k AS k1, timestamp_floor_utc('3h', timestamp, null, '00:15', 'CET') AS timestamp]
                                values: [sum(a) AS sum]
                                Scan
                                  table: x
                                  columns: [a, b, k, timestamp]
                        """);
    }

    @Test
    public void testCalendarWithOffsetNegative() throws Exception {
        assertQuery("select b, sum(a), k k1, k from x y sample by 3h align to calendar with offset '-04:45'")
                .ddl(DDL)
                .assertsLogicalPlan("""
                        Project
                          columns: [b, sum, k1, k]
                          Sort
                            keys: [timestamp]
                            Project
                              columns: [b, sum, k1, k1 AS k, timestamp]
                              Aggregate
                                keys: [b, k AS k1, timestamp_floor_utc('3h', timestamp, null, '-04:45', null) AS timestamp]
                                values: [sum(a) AS sum]
                                Scan
                                  table: x
                                  columns: [a, b, k, timestamp]
                        """);
    }

    @Test
    public void testCalendarWithOffsetPositive() throws Exception {
        assertQuery("select b, sum(a), k k1, k from x y sample by 3h align to calendar with offset '01:45'")
                .ddl(DDL)
                .assertsLogicalPlan("""
                        Project
                          columns: [b, sum, k1, k]
                          Sort
                            keys: [timestamp]
                            Project
                              columns: [b, sum, k1, k1 AS k, timestamp]
                              Aggregate
                                keys: [b, k AS k1, timestamp_floor_utc('3h', timestamp, null, '01:45', null) AS timestamp]
                                values: [sum(a) AS sum]
                                Scan
                                  table: x
                                  columns: [a, b, k, timestamp]
                        """);
    }

    @Test
    public void testFirstObservation() throws Exception {
        assertQuery("select b, sum(a), k k1, k from x y sample by 3h align to first observation")
                .ddl(DDL)
                .assertsLogicalPlan("""
                        Project
                          columns: [b, sum, k1, k1 AS k]
                          SampleBy
                            period: 3h
                            keys: [b, k AS k1]
                            values: [sum(a) AS sum]
                            Scan
                              table: x
                              columns: [a, b, k, timestamp]
                        """);
    }

    @Test
    public void testObservationExpected() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to first move",
                63,
                "'observation' expected",
                model()
        );
    }

    @Test
    public void testObservationMissing() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to first",
                62,
                "'observation' expected",
                model()
        );
    }

    @Test
    public void testSampleByAlignOn() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align on calendar",
                54,
                "'to' expected",
                model()
        );
    }

    @Test
    public void testSampleByMissing() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample on 3h",
                42,
                "'by' expected",
                model()
        );
    }

    @Test
    public void testSampleByToLastObservation() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to last observation",
                57,
                "'calendar' or 'first observation' expected",
                model()
        );
    }

    @Test
    public void testToMissing() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align",
                53,
                "'to' expected",
                model()
        );
    }

    @Test
    public void testUnqualifiedAlign() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to",
                56,
                "'calendar' or 'first observation' expected",
                model()
        );
    }

    private static TableModel model() {
        return modelOf("x")
                .col("a", ColumnType.DOUBLE)
                .col("b", ColumnType.SYMBOL)
                .col("k", ColumnType.TIMESTAMP)
                .timestamp();
    }
}
