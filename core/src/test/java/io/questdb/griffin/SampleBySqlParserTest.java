/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableModel;
import org.junit.Test;

public class SampleBySqlParserTest  extends AbstractSqlParserTest {

    @Test
    public void testCalendar() throws SqlException {
        assertQuery(
                "select-group-by b, sum(a) sum, k1, k1 k from (select-choose [b, a, k k1] b, a, k k1, timestamp from (select [b, a, k] from x y timestamp (timestamp)) y) y sample by 3h align to calendar with offset '00:00'",
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar",
                model()
        );
    }

    @Test
    public void testCalendarTimeZoneAndOffsetAsBindVariables() throws SqlException {
        assertQuery(
                "select-group-by b, sum(a) sum, k1, k1 k from (select-choose [b, a, k k1] b, a, k k1, timestamp from (select [b, a, k] from x y timestamp (timestamp)) y) y sample by 3h align to calendar time zone ? with offset ?",
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone ? with offset ?",
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
    public void testSampleByAlignOn() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align on calendar",
                54,
                "'to' expected",
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
    public void testCalendarTimeZoneAsOffset() throws SqlException {
        assertQuery(
                "select-group-by b, sum(a) sum, k1, k1 k from (select-choose [b, a, k k1] b, a, k k1, timestamp from (select [b, a, k] from x y timestamp (timestamp)) y) y sample by 3h align to calendar time zone '+01:00' with offset '00:00'",
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone '+01:00'",
                model()
        );
    }

    @Test
    public void testCalendarTimeZoneAsOffsetNegative() throws SqlException {
        assertQuery(
                "select-group-by b, sum(a) sum, k1, k1 k from (select-choose [b, a, k k1] b, a, k k1, timestamp from (select [b, a, k] from x y timestamp (timestamp)) y) y sample by 3h align to calendar time zone '-04:00' with offset '00:00'",
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone '-04:00'",
                model()
        );
    }

    @Test
    public void testFillFollowedByAlign() throws SqlException {
        assertQuery(
                "select-group-by b, sum(a) sum, k1, k1 k from (select-choose [b, a, k k1] b, a, k k1, timestamp from (select [b, a, k] from x y timestamp (timestamp)) y) y sample by 3h fill(none) align to calendar time zone ? with offset ?",
                "select b, sum(a), k k1, k from x y sample by 3h fill(none) align to calendar time zone ? with offset ?",
                model()
        );
    }

    @Test
    public void testCalendarTimeZone() throws SqlException {
        assertQuery(
                "select-group-by b, sum(a) sum, k1, k1 k from (select-choose [b, a, k k1] b, a, k k1, timestamp from (select [b, a, k] from x y timestamp (timestamp)) y) y sample by 3h align to calendar time zone 'CET' with offset '00:00'",
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone 'CET'",
                model()
        );
    }

    @Test
    public void testCalendarTimeZoneWithOffsetPositive() throws SqlException {
        assertQuery(
                "select-group-by b, sum(a) sum, k1, k1 k from (select-choose [b, a, k k1] b, a, k k1, timestamp from (select [b, a, k] from x y timestamp (timestamp)) y) y sample by 3h align to calendar time zone 'CET' with offset '00:15'",
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone 'CET' with offset '00:15'",
                model()
        );
    }

    @Test
    public void testCalendarTimeZoneWithOffsetNegative() throws SqlException {
        assertQuery(
                "select-group-by b, sum(a) sum, k1, k1 k from (select-choose [b, a, k k1] b, a, k k1, timestamp from (select [b, a, k] from x y timestamp (timestamp)) y) y sample by 3h align to calendar time zone 'CET' with offset '-00:15'",
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone 'CET' with offset '-00:15'",
                model()
        );
    }

    @Test
    public void testCalendarWithOffsetPositive() throws SqlException {
        assertQuery(
                "select-group-by b, sum(a) sum, k1, k1 k from (select-choose [b, a, k k1] b, a, k k1, timestamp from (select [b, a, k] from x y timestamp (timestamp)) y) y sample by 3h align to calendar with offset '01:45'",
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar with offset '01:45'",
                model()
        );
    }

    @Test
    public void testCalendarWithOffsetNegative() throws SqlException {
        assertQuery(
                "select-group-by b, sum(a) sum, k1, k1 k from (select-choose [b, a, k k1] b, a, k k1, timestamp from (select [b, a, k] from x y timestamp (timestamp)) y) y sample by 3h align to calendar with offset '-04:45'",
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar with offset '-04:45'",
                model()
        );
    }

    @Test
    public void testFirstObservation() throws SqlException {
        assertQuery(
                "select-group-by b, sum(a) sum, k1, k1 k from (select-choose [b, a, k k1] b, a, k k1, timestamp from (select [b, a, k] from x y timestamp (timestamp)) y) y sample by 3h",
                "select b, sum(a), k k1, k from x y sample by 3h align to first observation",
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
    public void testObservationExpected() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to first move",
                63,
                "'observation' expected",
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
    public void testAlignExpected() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h hello",
                48,
                "unexpected token: hello",
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
    public void testAlignToCalendarFollowedByInvalid() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar blah",
                66,
                "'time zone' or 'with offset' expected",
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
    public void testAlignToCalendarTimeZoneMissingZoneName() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone",
                75,
                "Expression expected",
                model()
        );
    }

    @Test
    public void testAlignToCalendarTimeZoneFollowedByUnexpectedToken() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone 'X' zone",
                80,
                "'with offset' expected",
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
    public void testAlignToCalendarTimeZoneWithSomethingUnexpected() throws Exception {
        assertSyntaxError(
                "select b, sum(a), k k1, k from x y sample by 3h align to calendar time zone 'X' with friends",
                85,
                "'offset' expected",
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
    private static TableModel model() {
        return modelOf("x")
                .col("a", ColumnType.DOUBLE)
                .col("b", ColumnType.SYMBOL)
                .col("k", ColumnType.TIMESTAMP)
                .timestamp();
    }
}
