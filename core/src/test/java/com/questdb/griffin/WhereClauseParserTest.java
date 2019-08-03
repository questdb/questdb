/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin;

import com.questdb.cairo.*;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.griffin.model.ExpressionNode;
import com.questdb.griffin.model.IntrinsicModel;
import com.questdb.griffin.model.QueryModel;
import com.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.questdb.griffin.GriffinParserTestUtils.intervalToString;

public class WhereClauseParserTest extends AbstractCairoTest {

    private final static SqlCompiler compiler = new SqlCompiler(new CairoEngine(configuration));
    private static TableReader reader;
    private static TableReader noTimestampReader;
    private static TableReader unindexedReader;
    private static RecordMetadata metadata;
    private static RecordMetadata noTimestampMetadata;
    private static RecordMetadata unindexedMetadata;
    private final RpnBuilder rpn = new RpnBuilder();
    private final WhereClauseParser e = new WhereClauseParser();
    private final PostOrderTreeTraversalAlgo traversalAlgo = new PostOrderTreeTraversalAlgo();
    private final PostOrderTreeTraversalAlgo.Visitor rpnBuilderVisitor = rpn::onNode;
    private final QueryModel queryModel = QueryModel.FACTORY.newInstance();

    @BeforeClass
    public static void setUp2() {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)) {
            model.col("sym", ColumnType.SYMBOL).indexed(true, 16)
                    .col("bid", ColumnType.DOUBLE)
                    .col("ask", ColumnType.DOUBLE)
                    .col("bidSize", ColumnType.INT)
                    .col("askSize", ColumnType.INT)
                    .col("mode", ColumnType.SYMBOL).indexed(true, 4)
                    .col("ex", ColumnType.SYMBOL).indexed(true, 4)
                    .timestamp();
            CairoTestUtils.create(model);
        }

        try (TableModel model = new TableModel(configuration, "y", PartitionBy.NONE)) {
            model.col("sym", ColumnType.SYMBOL).indexed(true, 16)
                    .col("bid", ColumnType.DOUBLE)
                    .col("ask", ColumnType.DOUBLE)
                    .col("bidSize", ColumnType.INT)
                    .col("askSize", ColumnType.INT)
                    .col("mode", ColumnType.SYMBOL).indexed(true, 4)
                    .col("ex", ColumnType.SYMBOL).indexed(true, 4);
            CairoTestUtils.create(model);
        }

        try (TableModel model = new TableModel(configuration, "z", PartitionBy.NONE)) {
            model.col("sym", ColumnType.SYMBOL)
                    .col("bid", ColumnType.DOUBLE)
                    .col("ask", ColumnType.DOUBLE)
                    .col("bidSize", ColumnType.INT)
                    .col("askSize", ColumnType.INT)
                    .col("mode", ColumnType.SYMBOL)
                    .col("ex", ColumnType.SYMBOL).indexed(true, 4)
                    .timestamp();
            CairoTestUtils.create(model);
        }

        reader = new TableReader(configuration, "x");
        metadata = reader.getMetadata();

        noTimestampReader = new TableReader(configuration, "y");
        noTimestampMetadata = noTimestampReader.getMetadata();

        unindexedReader = new TableReader(configuration, "z");
        unindexedMetadata = unindexedReader.getMetadata();
    }

    @AfterClass
    public static void tearDown2() {
        reader.close();
        noTimestampReader.close();
        unindexedReader.close();
    }

    @Test
    public void testAndBranchWithNonIndexedField() throws Exception {
        IntrinsicModel m = modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\") and bid > 100");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
        assertFilter(m, "100bid>");
    }

    @Test
    public void testBadCountInInterval() {
        try {
            modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m;10;z'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testBadDate() {
        try {
            modelOf("timestamp = '2015-02-23T10:00:55.00z;30m'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testBadDateInGreater() {
        try {
            modelOf("'2014-0x-01T12:30:00.000Z' > timestamp");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(0, e.getPosition());
        }
    }

    @Test
    public void testBadDateInGreater2() {
        try {
            modelOf("timestamp > '2014-0x-01T12:30:00.000Z'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testBadDateInInterval() {
        try {
            modelOf("timestamp = '2014-0x-01T12:30:00.000Z'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testBadEndDate() {
        try {
            modelOf("timestamp in (\"2014-01-02T12:30:00.000Z\", \"2014-01Z\")");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Invalid date");
            Assert.assertEquals(42, e.getPosition());
        }
    }

    @Test
    public void testBadOperators() {
        testBadOperator(">");
        testBadOperator(">=");
        testBadOperator("<");
        testBadOperator("<=");
        testBadOperator("=");
        testBadOperator("!=");
    }

    @Test
    public void testBadPeriodInInterval() {
        try {
            modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m;x;5'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testBadPeriodInInterval2() {
        try {
            modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m;10x;5'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testBadRangeInInterval() {
        try {
            modelOf("timestamp = '2014-03-01T12:30:00.000Z;x'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testBadStartDate() {
        try {
            modelOf("timestamp in (\"2014-01Z\", \"2014-01-02T12:30:00.000Z\")");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Invalid date");
            Assert.assertEquals(14, e.getPosition());
        }
    }

    @Test
    public void testComplexInterval1() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00;2d'");
        TestUtils.assertEquals("[{lo=2015-02-23T10:00:00.000000Z, hi=2015-02-25T10:00:59.999999Z}]", intervalToString(m.intervals));
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', filter=null}", m.toString());
    }

    @Test
    public void testComplexInterval2() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;7d'");
        TestUtils.assertEquals("[{lo=2015-02-23T10:00:55.000000Z, hi=2015-03-02T10:00:55.000000Z}]", intervalToString(m.intervals));
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', filter=null}", m.toString());
    }

    @Test
    public void testComplexInterval3() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;15s'");
        TestUtils.assertEquals("[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:01:10.000000Z}]", intervalToString(m.intervals));
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', filter=null}", m.toString());
    }

    @Test
    public void testComplexInterval4() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m'");
        TestUtils.assertEquals("[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:30:55.000000Z}]", intervalToString(m.intervals));
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', filter=null}", m.toString());
    }

    @Test
    public void testComplexInterval5() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m' and timestamp != '2015-02-23T10:10:00.000Z'");
        TestUtils.assertEquals("[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:09:59.999999Z},{lo=2015-02-23T10:10:00.000001Z, hi=2015-02-23T10:30:55.000000Z}]",
                intervalToString(m.intervals));
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', filter=null}", m.toString());
    }

    @Test
    public void testConstVsLambda() throws Exception {
        IntrinsicModel m = modelOf("ex in (1,2) and sym in (select * from xyz)");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertNotNull(m.keySubQuery);
        Assert.assertNotNull(m.filter);
        TestUtils.assertEquals("ex in (1,2)", GriffinParserTestUtils.toRpn(m.filter));
    }

    @Test
    public void testConstVsLambda2() throws Exception {
        IntrinsicModel m = modelOf("sym in (1,2) and sym in (select * from xyz)");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertNotNull(m.keySubQuery);
        Assert.assertNotNull(m.filter);
        TestUtils.assertEquals("sym in (1,2)", GriffinParserTestUtils.toRpn(m.filter));
    }

    @Test
    public void testContradictingNullSearch() throws Exception {
        IntrinsicModel m = modelOf("sym = null and sym != null and ex != 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, "'blah'ex!=");
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testContradictingNullSearch2() throws Exception {
        IntrinsicModel m = modelOf("null = sym and null != sym and ex != 'blah'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        assertFilter(m, "'blah'ex!=");
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testDubiousEquals() throws Exception {
        IntrinsicModel m = modelOf("sum(ts) = sum(ts)");
        Assert.assertNull(m.filter);
    }

    @Test
    public void testDubiousGreater() throws Exception {
        IntrinsicModel m = modelOf("ts > ts");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testDubiousLess() throws Exception {
        IntrinsicModel m = modelOf("ts < ts");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testDubiousNotEquals() throws Exception {
        IntrinsicModel m = modelOf("ts != ts");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testEqualsChoiceOfColumns() throws Exception {
        IntrinsicModel m = modelOf("sym = 'X' and ex = 'Y'");
        assertFilter(m, "'Y'ex=");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X]", m.keyValues.toString());
    }

    @Test
    public void testEqualsChoiceOfColumns2() throws Exception {
        IntrinsicModel m = modelOf("sym = 'X' and ex = 'Y'");
        assertFilter(m, "'Y'ex=");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X]", m.keyValues.toString());
    }

    @Test
    public void testEqualsIndexedSearch() throws Exception {
        IntrinsicModel m = modelOf("sym ='X' and bid > 100.05");
        assertFilter(m, "100.05bid>");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X]", m.keyValues.toString());
    }

    @Test
    public void testEqualsInvalidColumn() {
        try {
            modelOf("sym = 'X' and x = 'Y'");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            Assert.assertEquals(14, e.getPosition());
        }
    }

    @Test
    public void testEqualsNull() throws Exception {
        IntrinsicModel m = modelOf("sym = null");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[null]", m.keyValues.toString());
    }

    @Test
    public void testEqualsOverlapWithIn() throws Exception {
        IntrinsicModel m = modelOf("sym in ('x','y') and sym = 'y'");
        Assert.assertNull(m.filter);
        Assert.assertEquals("[y]", m.keyValues.toString());
        Assert.assertEquals("[12]", m.keyValuePositions.toString());
    }

    @Test
    public void testEqualsOverlapWithIn2() throws Exception {
        IntrinsicModel m = modelOf("sym = 'y' and sym in ('x','y')");
        Assert.assertNull(m.filter);
        Assert.assertEquals("[y]", m.keyValues.toString());
    }

    @Test
    public void testEqualsOverlapWithIn3() throws Exception {
        IntrinsicModel m = modelOf("sym in ('x','y') and sym = 'y'", "ex");
        TestUtils.assertEquals("'y'sym='y''x'syminand", toRpn(m.filter));
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testEqualsZeroOverlapWithIn() throws Exception {
        IntrinsicModel m = modelOf("sym in ('x','y') and sym = 'z'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testEqualsZeroOverlapWithIn2() throws Exception {
        IntrinsicModel m = modelOf("sym = 'z' and sym in ('x','y')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testExactDate() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp < '2015-05-11T08:00:55.000Z'");
        TestUtils.assertEquals("[{lo=2015-05-10T15:03:10.000000Z, hi=2015-05-10T15:03:10.000000Z}]", intervalToString(m.intervals));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testExactDateVsInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-11'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
        Assert.assertNull(m.filter);
    }

    @Test
    public void testFilterAndInterval() throws Exception {
        IntrinsicModel m = modelOf("bid > 100 and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
        assertFilter(m, "100bid>");
    }

    @Test
    public void testFilterMultipleKeysAndInterval() throws Exception {
        IntrinsicModel m = modelOf("sym in (\"a\", \"b\", \"c\") and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[a,b,c]", m.keyValues.toString());
        Assert.assertEquals("[8,13,18]", m.keyValuePositions.toString());
        Assert.assertNull(m.filter);
    }

    @Test
    public void testFilterOnIndexedFieldAndInterval() throws Exception {
        IntrinsicModel m = modelOf("sym in ('a') and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[a]", m.keyValues.toString());
        Assert.assertNull(m.filter);
    }

    @Test
    public void testFilterOrInterval() throws Exception {
        IntrinsicModel m = modelOf("bid > 100 or timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertNull(m.intervals);
        assertFilter(m, "\"2014-01-02T12:30:00.000Z\"\"2014-01-01T12:30:00.000Z\"timestampin100bid>or");
    }

    @Test
    public void testInNull() throws Exception {
        IntrinsicModel m = modelOf("sym in ('X', null, 'Y')");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X,null,Y]", m.keyValues.toString());
    }

    @Test
    public void testInVsEqualInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and timestamp = '2014-01-01'");
        Assert.assertNull(m.filter);
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-01T23:59:59.999999Z}]", intervalToString(m.intervals));
    }

    @Test
    public void testIndexedFieldTooFewArgs2() throws Exception {
        assertFilter(modelOf("sym in (x)"), "xsymin");
    }

    @Test
    public void testIndexedFieldTooFewArgs3() {
        try {
            modelOf("sym in ()");
            Assert.fail("exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Too few arguments");
        }
    }

    @Test
    public void testIntervalDontIntersect() throws Exception {
        // because of intervals being processed from right to left
        // code will try to intersect 'not equal' will already  existing positive interval
        // result must be zero-overlap and FALSE model
        IntrinsicModel m = modelOf("timestamp != '2015-05-11' and timestamp = '2015-05-11'");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testIntervalGreater1() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and timestamp > '2014-01-01T15:30:00.000Z'");
        TestUtils.assertEquals("[{lo=2014-01-01T15:30:00.000001Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
    }

    @Test
    public void testIntervalGreater2() throws Exception {
        IntrinsicModel m = modelOf("timestamp > '2014-01-01T15:30:00.000Z' and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        TestUtils.assertEquals("[{lo=2014-01-01T15:30:00.000001Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
    }

    @Test
    public void testIntervalGreater3() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and timestamp > x()");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
        TestUtils.assertEquals("xtimestamp>", toRpn(m.filter));
    }

    @Test
    public void testIntervalGreater4() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and x() > timestamp");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
        TestUtils.assertEquals("timestampx>", toRpn(m.filter));
    }

    @Test
    public void testIntervalGreater5() throws Exception {
        IntrinsicModel m = noTimestampModelOf("timestamp > '2014-01-01T15:30:00.000Z'");
        Assert.assertNull(m.intervals);
        TestUtils.assertEquals("'2014-01-01T15:30:00.000Z'timestamp>", toRpn(m.filter));
    }

    @Test
    public void testIntervalGreaterOrEq1() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and timestamp >= '2014-01-01T15:30:00.000Z'");
        TestUtils.assertEquals("[{lo=2014-01-01T15:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
    }

    @Test
    public void testIntervalGreaterOrEq2() throws Exception {
        IntrinsicModel m = modelOf("timestamp >= '2014-01-01T15:30:00.000Z' and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        TestUtils.assertEquals("[{lo=2014-01-01T15:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
    }

    @Test
    public void testIntervalLessNoTimestamp() throws Exception {
        IntrinsicModel m = noTimestampModelOf("timestamp < '2014-01-01T15:30:00.000Z'");
        Assert.assertNull(m.intervals);
        TestUtils.assertEquals("'2014-01-01T15:30:00.000Z'timestamp<", toRpn(m.filter));
    }

    @Test
    public void testIntervalSourceDay() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m;2d;5'");
        TestUtils.assertEquals("[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:30:55.000000Z},{lo=2015-02-25T10:00:55.000000Z, hi=2015-02-25T10:30:55.000000Z},{lo=2015-02-27T10:00:55.000000Z, hi=2015-02-27T10:30:55.000000Z},{lo=2015-03-01T10:00:55.000000Z, hi=2015-03-01T10:30:55.000000Z},{lo=2015-03-03T10:00:55.000000Z, hi=2015-03-03T10:30:55.000000Z}]",
                intervalToString(m.intervals));
    }

    @Test
    public void testIntervalSourceHour() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;10m;3h;10'");
        final String expected = "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:10:55.000000Z}," +
                "{lo=2015-02-23T13:00:55.000000Z, hi=2015-02-23T13:10:55.000000Z}," +
                "{lo=2015-02-23T16:00:55.000000Z, hi=2015-02-23T16:10:55.000000Z}," +
                "{lo=2015-02-23T19:00:55.000000Z, hi=2015-02-23T19:10:55.000000Z}," +
                "{lo=2015-02-23T22:00:55.000000Z, hi=2015-02-23T22:10:55.000000Z}," +
                "{lo=2015-02-24T01:00:55.000000Z, hi=2015-02-24T01:10:55.000000Z}," +
                "{lo=2015-02-24T04:00:55.000000Z, hi=2015-02-24T04:10:55.000000Z}," +
                "{lo=2015-02-24T07:00:55.000000Z, hi=2015-02-24T07:10:55.000000Z}," +
                "{lo=2015-02-24T10:00:55.000000Z, hi=2015-02-24T10:10:55.000000Z}," +
                "{lo=2015-02-24T13:00:55.000000Z, hi=2015-02-24T13:10:55.000000Z}]";

        TestUtils.assertEquals(expected, intervalToString(m.intervals));
    }

    @Test
    public void testIntervalSourceMin() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;15s;15m;5'");
        final String expected = "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:01:10.000000Z}," +
                "{lo=2015-02-23T10:15:55.000000Z, hi=2015-02-23T10:16:10.000000Z}," +
                "{lo=2015-02-23T10:30:55.000000Z, hi=2015-02-23T10:31:10.000000Z}," +
                "{lo=2015-02-23T10:45:55.000000Z, hi=2015-02-23T10:46:10.000000Z}," +
                "{lo=2015-02-23T11:00:55.000000Z, hi=2015-02-23T11:01:10.000000Z}]";
        TestUtils.assertEquals(expected, intervalToString(m.intervals));
    }

    @Test
    public void testIntervalSourceMonth() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;2h;2M;3'");
        final String expected = "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T12:00:55.000000Z}," +
                "{lo=2015-04-23T10:00:55.000000Z, hi=2015-04-23T12:00:55.000000Z}," +
                "{lo=2015-06-23T10:00:55.000000Z, hi=2015-06-23T12:00:55.000000Z}]";

        TestUtils.assertEquals(expected, intervalToString(m.intervals));
    }

    @Test
    public void testIntervalSourceSec() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;5s;30s;5'");
        final String expected = "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-23T10:01:00.000000Z}," +
                "{lo=2015-02-23T10:01:25.000000Z, hi=2015-02-23T10:01:30.000000Z}," +
                "{lo=2015-02-23T10:01:55.000000Z, hi=2015-02-23T10:02:00.000000Z}," +
                "{lo=2015-02-23T10:02:25.000000Z, hi=2015-02-23T10:02:30.000000Z}," +
                "{lo=2015-02-23T10:02:55.000000Z, hi=2015-02-23T10:03:00.000000Z}]";

        TestUtils.assertEquals(expected, intervalToString(m.intervals));
    }

    @Test
    public void testIntervalSourceYear() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;1d;1y;5'");
        final String expected = "[{lo=2015-02-23T10:00:55.000000Z, hi=2015-02-24T10:00:55.000000Z}," +
                "{lo=2016-02-23T10:00:55.000000Z, hi=2016-02-24T10:00:55.000000Z}," +
                "{lo=2017-02-23T10:00:55.000000Z, hi=2017-02-24T10:00:55.000000Z}," +
                "{lo=2018-02-23T10:00:55.000000Z, hi=2018-02-24T10:00:55.000000Z}," +
                "{lo=2019-02-23T10:00:55.000000Z, hi=2019-02-24T10:00:55.000000Z}]";

        TestUtils.assertEquals(expected, intervalToString(m.intervals));
    }

    @Test
    public void testIntervalTooFewArgs() {
        try {
            modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\")");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Too few arg");
        }
    }

    @Test
    public void testIntervalTooFewArgs2() {
        try {
            modelOf("timestamp in ()");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Too few arg");
        }
    }

    @Test
    public void testIntervalTooManyArgs() {
        try {
            modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\", \"2014-01-03T12:30:00.000Z\")");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Too many arg");
        }
    }

    @Test
    public void testIntrinsicPickup() throws Exception {
        assertFilter(modelOf("timestamp = '2014-06-20T13:25:00.000Z;10m;2d;4' and sym in ('A', 'B') or ex = 'D'"), "'D'ex='B''A'symin'2014-06-20T13:25:00.000Z;10m;2d;4'timestamp=andor");
        assertFilter(modelOf("timestamp = '2014-06-20T13:25:00.000Z;10m;2d;4' or ex = 'D' and sym in ('A', 'B')"), "'D'ex='2014-06-20T13:25:00.000Z;10m;2d;4'timestamp=or");
    }

    @Test(expected = SqlException.class)
    public void testInvalidIntervalSource1() throws Exception {
        modelOf("timestamp = '2014-06-20T13:25:00.000Z;10m;2d'");
    }

    @Test(expected = SqlException.class)
    public void testInvalidIntervalSource2() throws Exception {
        modelOf("timestamp = '2014-06-20T13:25:00.000Z;10m;2d;4;4'");
    }

    @Test
    public void testLambdaVsConst() throws Exception {
        IntrinsicModel m = modelOf("sym in (select a from xyz) and ex in (1,2)");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertNotNull(m.keySubQuery);
        Assert.assertNotNull(m.filter);
        TestUtils.assertEquals("ex in (1,2)", GriffinParserTestUtils.toRpn(m.filter));
    }

    @Test
    public void testLambdaVsLambda() throws Exception {
        IntrinsicModel m = modelOf("ex in (select * from abc) and sym in (select * from xyz)");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertNotNull(m.keySubQuery);
        Assert.assertNotNull(m.filter);
        TestUtils.assertEquals("ex in (select-choose * column from (abc))", GriffinParserTestUtils.toRpn(m.filter));
    }

    @Test
    public void testLessInvalidDate() {
        try {
            modelOf("timestamp < '2014-0x-01T12:30:00.000Z'");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testLessInvalidDate2() {
        try {
            modelOf("'2014-0x-01T12:30:00.000Z' < timestamp");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(0, e.getPosition());
        }
    }

    @Test
    public void testLessNonConstant() throws SqlException {
        IntrinsicModel m = modelOf("timestamp < x");
        Assert.assertNull(m.intervals);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        TestUtils.assertEquals("xtimestamp<", toRpn(m.filter));
    }

    @Test
    public void testLessNonConstant2() throws SqlException {
        IntrinsicModel m = modelOf("x < timestamp");
        Assert.assertNull(m.intervals);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        TestUtils.assertEquals("timestampx<", toRpn(m.filter));
    }

    @Test
    public void testListOfValuesNegativeOverlap() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and sym in ('c')");
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testListOfValuesOverlapWithNotClause() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and not (sym in ('c', 'd', 'e'))");
        Assert.assertEquals("[a,z]", m.keyValues.toString());
        Assert.assertNull(m.filter);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testListOfValuesOverlapWithNotClause2() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and not (sym in ('a', 'd', 'e'))");
        Assert.assertNull(m.filter);
        Assert.assertEquals("[z]", m.keyValues.toString());
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testListOfValuesOverlapWithNotClause3() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and not (sym in ('a', 'z', 'e'))");
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testListOfValuesPositiveOverlap() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and sym in ('z')");
        Assert.assertNull(m.filter);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[z]", m.keyValues.toString());
    }

    @Test
    public void testListOfValuesPositiveOverlapQuoteIndifference() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', \"z\") and sym in ('z')");
        Assert.assertNull(m.filter);
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[z]", m.keyValues.toString());
    }

    @Test
    public void testLiteralInInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\", c)");
        Assert.assertNull(m.intervals);
        assertFilter(m, "c\"2014-01-01T12:30:00.000Z\"timestampin");
    }

    @Test
    public void testLiteralInListOfValues() throws Exception {
        IntrinsicModel m = modelOf("sym in (\"a\", z) and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
        Assert.assertNull(m.keyColumn);
        assertFilter(m, "z\"a\"symin");
    }

    @Test
    public void testLiteralInListOfValuesInvalidColumn() {
        try {
            modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and x in ('a', z)");
            Assert.fail("Exception expected");
        } catch (SqlException e) {
            Assert.assertEquals(74, e.getPosition());
        }
    }

    @Test
    public void testManualInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp >= '2014-01-01T15:30:00.000Z' and timestamp < '2014-01-02T12:30:00.000Z'");
        TestUtils.assertEquals("[{lo=2014-01-01T15:30:00.000000Z, hi=2014-01-02T12:29:59.999999Z}]", intervalToString(m.intervals));
    }

    @Test
    public void testManualIntervalInverted() throws Exception {
        IntrinsicModel m = modelOf("'2014-01-02T12:30:00.000Z' > timestamp and '2014-01-01T15:30:00.000Z' <= timestamp ");
        TestUtils.assertEquals("[{lo=2014-01-01T15:30:00.000000Z, hi=2014-01-02T12:29:59.999999Z}]", intervalToString(m.intervals));
    }

    @Test
    public void testMultipleAnds() throws Exception {
        IntrinsicModel m = modelOf("a > 10 and b > 20 and (c > 100 and d < 20 and bid = 30)");
        assertFilter(m, "30bid=20d<100c>andand20b>10a>andand");
    }

    @Test
    public void testNestedFunctionTest() throws Exception {
        IntrinsicModel m = modelOf("substr(parse(x, 1, 3), 2, 4)");
        Assert.assertNull(m.intervals);
        assertFilter(m, "4231xparsesubstr");
    }

    @Test
    public void testNoIntrinsics() throws Exception {
        IntrinsicModel m = modelOf("a > 10 or b > 20");
        Assert.assertNull(m.intervals);
        Assert.assertNull(m.keyColumn);
        assertFilter(m, "20b>10a>or");
    }

    @Test
    public void testNotEqualInvalidColumn() {
        try {
            modelOf("ex != null and abb != 'blah'");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "Invalid column");
            Assert.assertEquals(15, e.getPosition());
        }
    }

    @Test
    public void testNotEqualPreferredColumn() throws Exception {
        IntrinsicModel m = modelOf("sym = null and sym != null and ex != 'blah'", "ex");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "'blah'ex!=nullsym!=nullsym=andand");
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testNotEqualsDoesNotOverlapWithIn() throws Exception {
        IntrinsicModel m = modelOf("sym in ('x','y') and sym != 'z' and ex != 'blah'");
        assertFilter(m, "'blah'ex!=");
        Assert.assertEquals("[x,y]", m.keyValues.toString());
        Assert.assertEquals("[8,12]", m.keyValuePositions.toString());
    }

    @Test
    public void testNotEqualsOverlapWithIn() throws Exception {
        IntrinsicModel m = modelOf("sym in ('x','y') and sym != 'y' and ex != 'blah'");
        assertFilter(m, "'blah'ex!=");
        Assert.assertEquals("[x]", m.keyValues.toString());
        Assert.assertEquals("[8]", m.keyValuePositions.toString());
    }

    @Test
    public void testNotInIntervalIntersect() throws Exception {
        IntrinsicModel m = modelOf("not (timestamp in  ('2015-05-11T15:00:00.000Z', '2015-05-11T20:00:00.000Z')) and timestamp = '2015-05-11'");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        TestUtils.assertEquals("[{lo=2015-05-11T00:00:00.000000Z, hi=2015-05-11T14:59:59.999999Z},{lo=2015-05-11T20:00:00.000001Z, hi=2015-05-11T23:59:59.999999Z}]",
                intervalToString(m.intervals));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testNotInIntervalIntersect2() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-11' and not (timestamp in  ('2015-05-11T15:00:00.000Z', '2015-05-11T20:00:00.000Z'))");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        TestUtils.assertEquals("[{lo=2015-05-11T00:00:00.000000Z, hi=2015-05-11T14:59:59.999999Z},{lo=2015-05-11T20:00:00.000001Z, hi=2015-05-11T23:59:59.999999Z}]",
                intervalToString(m.intervals));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testNotInIntervalIntersect3() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-11' and not (timestamp in  ('2015-05-11T15:00:00.000Z', '2015-05-11T20:00:00.000Z')) and not (timestamp in ('2015-05-11T12:00:00.000Z', '2015-05-11T14:00:00.000Z')))");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        TestUtils.assertEquals("[{lo=2015-05-11T00:00:00.000000Z, hi=2015-05-11T11:59:59.999999Z},{lo=2015-05-11T14:00:00.000001Z, hi=2015-05-11T14:59:59.999999Z},{lo=2015-05-11T20:00:00.000001Z, hi=2015-05-11T23:59:59.999999Z}]",
                intervalToString(m.intervals));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testNotInIntervalInvalidHi() {
        try {
            modelOf("not (timestamp in  ('2015-05-11T15:00:00.000Z', 'abc')) and timestamp = '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid date");
            Assert.assertEquals(48, e.getPosition());
        }
    }

    @Test
    public void testNotInIntervalInvalidLo() {
        try {
            modelOf("not (timestamp in  ('abc','2015-05-11T15:00:00.000Z')) and timestamp = '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid date");
            Assert.assertEquals(20, e.getPosition());
        }
    }

    @Test
    public void testNotInIntervalNonConstant() throws SqlException {
        IntrinsicModel m = modelOf("not (timestamp in  (x, 'abc')) and timestamp = '2015-05-11'");
        TestUtils.assertEquals("[{lo=2015-05-11T00:00:00.000000Z, hi=2015-05-11T23:59:59.999999Z}]", intervalToString(m.intervals));
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        TestUtils.assertEquals("'abc'xtimestampinnot", toRpn(m.filter));
    }

    @Test
    public void testNotInIntervalNonLiteral() {
        try {
            modelOf("not (timestamp() in  ('2015-05-11T15:00:00.000Z')) and timestamp = '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Column name");
        }
    }

    @Test
    public void testNotInIntervalTooFew() {
        try {
            modelOf("not (timestamp in  ('2015-05-11T15:00:00.000Z')) and timestamp = '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Too few");
        }
    }

    @Test
    public void testNotInIntervalTooMany() {
        try {
            modelOf("not (timestamp in  ('2015-05-11T15:00:00.000Z','2015-05-11T15:00:00.000Z','2015-05-11T15:00:00.000Z')) and timestamp = '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Too many");
        }
    }

    @Test
    public void testNotInInvalidColumn() {
        try {
            modelOf("not (xyz in  ('2015-05-11T15:00:00.000Z')) and timestamp = '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column");
            Assert.assertEquals(5, e.getPosition());
        }
    }

    @Test
    public void testNotInTooFew() {
        try {
            modelOf("not (ex in  ()) and timestamp = '2015-05-11'");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Too few");
            Assert.assertEquals(8, e.getPosition());
        }
    }

    @Test
    public void testOr() throws Exception {
        IntrinsicModel m = modelOf("(sym = 'X' or sym = 'Y') and bid > 10");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "10bid>'Y'sym='X'sym=orand");
    }

    @Test
    public void testOrNullSearch() throws Exception {
        IntrinsicModel m = modelOf("sym = null or sym != null and ex != 'blah'");
        Assert.assertEquals(IntrinsicModel.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "'blah'ex!=nullsym!=nullsym=orand");
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testPreferredColumn() throws Exception {
        IntrinsicModel m;
        m = modelOf("sym in ('a', 'b') and ex in ('c') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110", "ex");
        assertFilter(m, "110ask<100bid>'b''a'syminandand");
        TestUtils.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[c]", m.keyValues.toString());
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
    }

    @Test
    public void testPreferredColumn2() throws Exception {
        IntrinsicModel m;
        m = modelOf("ex in ('c') and sym in ('a', 'b') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110", "ex");
        assertFilter(m, "110ask<100bid>'b''a'syminandand");
        TestUtils.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[c]", m.keyValues.toString());
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
    }

    @Test
    public void testPreferredColumn3() throws Exception {
        IntrinsicModel m;
        m = modelOf("sym in ('a', 'b') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110", "ex");
        assertFilter(m, "110ask<100bid>'b''a'syminandand");
        Assert.assertNull(m.keyColumn);
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
    }

    @Test
    public void testSimpleInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testSimpleLambda() throws Exception {
        IntrinsicModel m = modelOf("sym in (select * from xyz)");
        Assert.assertNotNull(m.keySubQuery);
    }

    @Test
    public void testSingleQuoteInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testThreeIntrinsics() throws Exception {
        IntrinsicModel m;
        m = modelOf("sym in ('a', 'b') and ex in ('c') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110");
//        assertFilter(m, "110ask<100bid>'c'exinandand");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[a,b]", m.keyValues.toString());
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
    }

    @Test
    public void testThreeIntrinsics2() throws Exception {
        IntrinsicModel m;
        m = modelOf("ex in ('c') and sym in ('a', 'b') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110");
        assertFilter(m, "110ask<100bid>'c'exinandand");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[a,b]", m.keyValues.toString());
        TestUtils.assertEquals("[{lo=2014-01-01T12:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
    }

    @Test
    public void testTwoDiffColLambdas() throws Exception {
        IntrinsicModel m = modelOf("sym in (select * from xyz) and ex in (select  * from kkk)");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertNotNull(m.keySubQuery);
        Assert.assertNotNull(m.filter);
        Assert.assertEquals(ExpressionNode.QUERY, m.filter.rhs.type);
    }

    @Test
    public void testTwoExactMatchDifferentDates() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-11T15:03:10.000Z' and timestamp = '2015-05-11'");
        TestUtils.assertEquals("[]", intervalToString(m.intervals));
        Assert.assertNull(m.filter);
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testTwoExactSameDates() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-11'");
        TestUtils.assertEquals("[]", intervalToString(m.intervals));
        Assert.assertNull(m.filter);
        Assert.assertEquals(IntrinsicModel.FALSE, m.intrinsicValue);
    }

    @Test
    public void testTwoIntervalSources() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2014-06-20T13:25:00.000Z;10m;2d;5' and timestamp = '2015-06-20T13:25:00.000Z;10m;2d;5'");
        TestUtils.assertEquals("[]", intervalToString(m.intervals));
    }

    @Test
    public void testTwoIntervals() throws Exception {
        IntrinsicModel m = modelOf("bid > 100 and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\") and timestamp in (\"2014-01-01T16:30:00.000Z\", \"2014-01-05T12:30:00.000Z\")");
        TestUtils.assertEquals("[{lo=2014-01-01T16:30:00.000000Z, hi=2014-01-02T12:30:00.000000Z}]", intervalToString(m.intervals));
    }

    @Test
    public void testTwoSameColLambdas() {
        try {
            modelOf("sym in (select * from xyz) and sym in (select * from kkk)");
            Assert.fail("exception expected");
        } catch (SqlException e) {
            Assert.assertEquals(4, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "Multiple lambda");
        }
    }

    @Test
    public void testUnindexedEquals() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym = 'ABC'", null);
        Assert.assertNull(m.keyColumn);
        TestUtils.assertEquals("sym = 'ABC'", GriffinParserTestUtils.toRpn(m.filter));
        TestUtils.assertEquals("[]", m.keyValues.toString());
    }

    @Test
    public void testUnindexedIn() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym in (1,2)", null);
        Assert.assertNull(m.keyColumn);
        TestUtils.assertEquals("sym in (1,2)", GriffinParserTestUtils.toRpn(m.filter));
        TestUtils.assertEquals("[]", m.keyValues.toString());
    }

    @Test
    public void testUnindexedPreferredEquals() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym = 'ABC'", "sym");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertNull(m.filter);
        TestUtils.assertEquals("[ABC]", m.keyValues.toString());
    }

    @Test
    public void testUnindexedPreferredIn() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym in (1,2)", "sym");
        TestUtils.assertEquals("sym", m.keyColumn);
        Assert.assertNull(m.filter);
        TestUtils.assertEquals("[1,2]", m.keyValues.toString());
    }

    @Test
    public void testUnindexedPreferredInVsIndexed() throws SqlException {
        IntrinsicModel m = unindexedModelOf("sym in (1,2) and ex in ('XYZ')", "sym");
        TestUtils.assertEquals("sym", m.keyColumn);
        TestUtils.assertEquals("ex in 'XYZ'", GriffinParserTestUtils.toRpn(m.filter));
        TestUtils.assertEquals("[1,2]", m.keyValues.toString());
    }

    private void assertFilter(IntrinsicModel m, CharSequence expected) throws SqlException {
        Assert.assertNotNull(m.filter);
        TestUtils.assertEquals(expected, toRpn(m.filter));
    }

    private IntrinsicModel modelOf(CharSequence seq) throws SqlException {
        return modelOf(seq, null);
    }

    private IntrinsicModel modelOf(CharSequence seq, String preferredColumn) throws SqlException {
        queryModel.clear();
        return e.extract(column -> column, compiler.testParseExpression(seq, queryModel), metadata, preferredColumn, metadata.getTimestampIndex());
    }

    private IntrinsicModel noTimestampModelOf(CharSequence seq) throws SqlException {
        queryModel.clear();
        return e.extract(column -> column, compiler.testParseExpression(seq, queryModel), noTimestampMetadata, null, noTimestampMetadata.getTimestampIndex());
    }

    private void testBadOperator(String op) {
        try {
            modelOf("sum(ts) " + op);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(8, e.getPosition());
        }

        try {
            modelOf(op + " sum(ts)");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(0, e.getPosition());
        }

    }

    private CharSequence toRpn(ExpressionNode node) throws SqlException {
        rpn.reset();
        traversalAlgo.traverse(node, rpnBuilderVisitor);
        return rpn.rpn();
    }

    private IntrinsicModel unindexedModelOf(CharSequence seq, String preferredColumn) throws SqlException {
        queryModel.clear();
        return e.extract(column -> column, compiler.testParseExpression(seq, queryModel), unindexedMetadata, preferredColumn, unindexedMetadata.getTimestampIndex());
    }
}
