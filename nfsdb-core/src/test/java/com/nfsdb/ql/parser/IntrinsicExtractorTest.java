/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ql.parser;

import com.nfsdb.JournalWriter;
import com.nfsdb.model.Quote;
import com.nfsdb.ql.model.ExprNode;
import com.nfsdb.ql.model.IntrinsicModel;
import com.nfsdb.ql.model.IntrinsicValue;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IntrinsicExtractorTest extends AbstractTest {

    private final RpnBuilder rpn = new RpnBuilder();
    private final ExprParser p = new ExprParser();
    private final AstBuilder ast = new AstBuilder();
    private final IntrinsicExtractor e = new IntrinsicExtractor();
    private final PostOrderTreeTraversalAlgo traversalAlgo = new PostOrderTreeTraversalAlgo();
    private final PostOrderTreeTraversalAlgo.Visitor rpnBuilderVisitor = new PostOrderTreeTraversalAlgo.Visitor() {
        @Override
        public void visit(ExprNode node) {
            rpn.onNode(node);
        }
    };
    private JournalWriter<Quote> w;

    @Before
    public void setUp() throws Exception {
        w = factory.writer(Quote.class);
    }

    @Test
    public void testAndBranchWithNonIndexedField() throws Exception {
        IntrinsicModel m = modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\") and bid > 100");
        Assert.assertTrue(m.intervalLo > Long.MIN_VALUE);
        Assert.assertTrue(m.intervalHi < Long.MAX_VALUE);
        assertFilter(m, "100bid>");
    }

    @Test
    public void testBadEndDate() throws Exception {
        try {
            modelOf("timestamp in (\"2014-01-02T12:30:00.000Z\", \"2014-01Z\")");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(e.getMessage().contains("Unknown date format"));
        }
    }

    @Test
    public void testBadStartDate() throws Exception {
        try {
            modelOf("timestamp in (\"2014-01Z\", \"2014-01-02T12:30:00.000Z\")");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(e.getMessage().contains("Unknown date format"));
        }
    }

    @Test
    public void testComplexInterval1() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00;2d'");
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', intervalLo=2015-02-23T10:00:00.000Z, intervalHi=2015-02-25T10:00:59.999Z, filter=null, millis=-9223372036854775808}", m.toString());
    }

    @Test
    public void testComplexInterval2() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;7d'");
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', intervalLo=2015-02-23T10:00:55.000Z, intervalHi=2015-03-02T10:00:55.000Z, filter=null, millis=-9223372036854775808}", m.toString());
    }

    @Test
    public void testComplexInterval3() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;15s'");
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', intervalLo=2015-02-23T10:00:55.000Z, intervalHi=2015-02-23T10:01:10.000Z, filter=null, millis=-9223372036854775808}", m.toString());
    }

    @Test
    public void testComplexInterval4() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m'");
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', intervalLo=2015-02-23T10:00:55.000Z, intervalHi=2015-02-23T10:30:55.000Z, filter=null, millis=-9223372036854775808}", m.toString());
    }

    @Test
    public void testEqualsChoiceOfColumns() throws Exception {
        IntrinsicModel m = modelOf("sym = 'X' and ex = 'Y'");
        assertFilter(m, "'Y'ex=");
        Assert.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X]", m.keyValues.toString());
    }

    @Test
    public void testEqualsChoiceOfColumns2() throws Exception {
        IntrinsicModel m = modelOf("ex = 'Y' and sym = 'X'");
        assertFilter(m, "'Y'ex=");
        Assert.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X]", m.keyValues.toString());
    }

    @Test
    public void testEqualsIndexedSearach() throws Exception {
        IntrinsicModel m = modelOf("sym ='X' and bid > 100.05");
        assertFilter(m, "100.05bid>");
        Assert.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X]", m.keyValues.toString());
    }

    @Test
    public void testEqualsInvalidColumn() throws Exception {
        try {
            modelOf("sym = 'X' and x = 'Y'");
            Assert.fail("Exception expected");
        } catch (ParserException e1) {
            Assert.assertEquals(14, e1.getPosition());
        }
    }

    @Test
    public void testEqualsNull() throws Exception {
        IntrinsicModel m = modelOf("sym = null");
        Assert.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[null]", m.keyValues.toString());
    }

    @Test
    public void testEqualsOverlapWithIn() throws Exception {
        IntrinsicModel m = modelOf("sym in ('x','y') and sym = 'y'");
        Assert.assertNull(m.filter);
        Assert.assertEquals("[y]", m.keyValues.toString());
    }

    @Test
    public void testEqualsOverlapWithIn2() throws Exception {
        IntrinsicModel m = modelOf("sym = 'y' and sym in ('x','y')");
        Assert.assertNull(m.filter);
        Assert.assertEquals("[y]", m.keyValues.toString());
    }

    @Test
    public void testEqualsZeroOverlapWithIn() throws Exception {
        IntrinsicModel m = modelOf("sym in ('x','y') and sym = 'z'");
        Assert.assertEquals(IntrinsicValue.FALSE, m.intrinsicValue);
    }

    @Test
    public void testEqualsZeroOverlapWithIn2() throws Exception {
        IntrinsicModel m = modelOf("sym = 'z' and sym in ('x','y')");
        Assert.assertEquals(IntrinsicValue.FALSE, m.intrinsicValue);
    }

    @Test
    public void testExactDate() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp < '2015-05-11T08:00:55.000Z'");
        assertFilter(m, "'2015-05-11T08:00:55.000Z'timestamp<");
        Assert.assertEquals("2015-05-10T15:03:10.000Z", Dates.toString(m.millis));
        Assert.assertEquals(Long.MIN_VALUE, m.intervalLo);
        Assert.assertEquals(Long.MAX_VALUE, m.intervalHi);
    }

    @Test
    public void testExactDateVsInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-11'");
        assertFilter(m, "'2015-05-11'timestamp=");
        Assert.assertEquals("2015-05-10T15:03:10.000Z", Dates.toString(m.millis));
        Assert.assertEquals(Long.MIN_VALUE, m.intervalLo);
        Assert.assertEquals(Long.MAX_VALUE, m.intervalHi);
        Assert.assertNull(m.intervalSource);
    }

    @Test
    public void testFilterAndInterval() throws Exception {
        IntrinsicModel m = modelOf("bid > 100 and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertTrue(m.intervalLo > Long.MIN_VALUE);
        Assert.assertTrue(m.intervalHi < Long.MAX_VALUE);
        assertFilter(m, "100bid>");
    }

    @Test
    public void testFilterMultipleKeysAndInterval() throws Exception {
        IntrinsicModel m = modelOf("sym in (\"a\", \"b\", \"c\") and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertTrue(m.intervalLo > Long.MIN_VALUE);
        Assert.assertTrue(m.intervalHi < Long.MAX_VALUE);
        Assert.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[a,b,c]", m.keyValues.toString());
        Assert.assertNull(m.filter);
    }

    @Test
    public void testFilterOnIndexedFieldAndInterval() throws Exception {
        IntrinsicModel m = modelOf("sym in ('a') and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertTrue(m.intervalLo > Long.MIN_VALUE);
        Assert.assertTrue(m.intervalHi < Long.MAX_VALUE);
        Assert.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[a]", m.keyValues.toString());
        Assert.assertNull(m.filter);
    }

    @Test
    public void testFilterOrInterval() throws Exception {
        IntrinsicModel m = modelOf("bid > 100 or timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertTrue(m.intervalLo == Long.MIN_VALUE);
        Assert.assertTrue(m.intervalHi == Long.MAX_VALUE);
        assertFilter(m, "\"2014-01-02T12:30:00.000Z\"\"2014-01-01T12:30:00.000Z\"timestampin100bid>or");
    }

    @Test
    public void testInNull() throws Exception {
        IntrinsicModel m = modelOf("sym in ('X', null, 'Y')");
        Assert.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X,null,Y]", m.keyValues.toString());
    }

    @Test
    public void testInVsEqualInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and timestamp = '2014-01-01'");
        Assert.assertNull(m.filter);
        Assert.assertEquals("2014-01-01T12:30:00.000Z", Dates.toString(m.intervalLo));
        Assert.assertEquals("2014-01-01T23:59:59.999Z", Dates.toString(m.intervalHi));
    }

    @Test
    public void testIndexedFieldTooFewArgs2() throws Exception {
        assertFilter(modelOf("sym in (x)"), "xsymin");
    }

    @Test
    public void testIndexedFieldTooFewArgs3() throws Exception {
        try {
            modelOf("sym in ()");
            Assert.fail("exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(e.getMessage().contains("Too few arguments"));
        }
    }

    @Test
    public void testIntervalGreater1() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and timestamp > '2014-01-01T15:30:00.000Z'");
        Assert.assertEquals("2014-01-01T15:30:00.001Z", Dates.toString(m.intervalLo));
        Assert.assertEquals("2014-01-02T12:30:00.000Z", Dates.toString(m.intervalHi));
    }

    @Test
    public void testIntervalGreater2() throws Exception {
        IntrinsicModel m = modelOf("timestamp > '2014-01-01T15:30:00.000Z' and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        Assert.assertEquals("2014-01-01T15:30:00.001Z", Dates.toString(m.intervalLo));
        Assert.assertEquals("2014-01-02T12:30:00.000Z", Dates.toString(m.intervalHi));
    }

    @Test
    public void testIntervalGreaterOrEq1() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and timestamp >= '2014-01-01T15:30:00.000Z'");
        Assert.assertEquals("2014-01-01T15:30:00.000Z", Dates.toString(m.intervalLo));
        Assert.assertEquals("2014-01-02T12:30:00.000Z", Dates.toString(m.intervalHi));
    }

    @Test
    public void testIntervalGreaterOrEq2() throws Exception {
        IntrinsicModel m = modelOf("timestamp >= '2014-01-01T15:30:00.000Z' and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        Assert.assertEquals("2014-01-01T15:30:00.000Z", Dates.toString(m.intervalLo));
        Assert.assertEquals("2014-01-02T12:30:00.000Z", Dates.toString(m.intervalHi));
    }

    @Test
    public void testIntervalSourceDay() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m;2d;5'");
        Assert.assertNotNull(m.intervalSource);

        final String expected = "Interval{lo=2015-02-23T10:00:55.000Z, hi=2015-02-23T10:30:55.000Z}\n" +
                "Interval{lo=2015-02-25T10:00:55.000Z, hi=2015-02-25T10:30:55.000Z}\n" +
                "Interval{lo=2015-02-27T10:00:55.000Z, hi=2015-02-27T10:30:55.000Z}\n" +
                "Interval{lo=2015-03-01T10:00:55.000Z, hi=2015-03-01T10:30:55.000Z}\n" +
                "Interval{lo=2015-03-03T10:00:55.000Z, hi=2015-03-03T10:30:55.000Z}\n";

        StringBuilder b = new StringBuilder();
        for (Interval in : m.intervalSource) {
            b.append(in.toString());
            b.append('\n');
        }

        Assert.assertEquals(expected, b.toString());
    }

    @Test
    public void testIntervalSourceHour() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;10m;3h;10'");
        Assert.assertNotNull(m.intervalSource);

        final String expected = "Interval{lo=2015-02-23T10:00:55.000Z, hi=2015-02-23T10:10:55.000Z}\n" +
                "Interval{lo=2015-02-23T13:00:55.000Z, hi=2015-02-23T13:10:55.000Z}\n" +
                "Interval{lo=2015-02-23T16:00:55.000Z, hi=2015-02-23T16:10:55.000Z}\n" +
                "Interval{lo=2015-02-23T19:00:55.000Z, hi=2015-02-23T19:10:55.000Z}\n" +
                "Interval{lo=2015-02-23T22:00:55.000Z, hi=2015-02-23T22:10:55.000Z}\n" +
                "Interval{lo=2015-02-24T01:00:55.000Z, hi=2015-02-24T01:10:55.000Z}\n" +
                "Interval{lo=2015-02-24T04:00:55.000Z, hi=2015-02-24T04:10:55.000Z}\n" +
                "Interval{lo=2015-02-24T07:00:55.000Z, hi=2015-02-24T07:10:55.000Z}\n" +
                "Interval{lo=2015-02-24T10:00:55.000Z, hi=2015-02-24T10:10:55.000Z}\n" +
                "Interval{lo=2015-02-24T13:00:55.000Z, hi=2015-02-24T13:10:55.000Z}\n";

        StringBuilder b = new StringBuilder();
        for (Interval in : m.intervalSource) {
            b.append(in.toString());
            b.append('\n');
        }

        Assert.assertEquals(expected, b.toString());
    }

    @Test
    public void testIntervalSourceMin() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;15s;15m;5'");
        Assert.assertNotNull(m.intervalSource);

        final String expected = "Interval{lo=2015-02-23T10:00:55.000Z, hi=2015-02-23T10:01:10.000Z}\n" +
                "Interval{lo=2015-02-23T10:15:55.000Z, hi=2015-02-23T10:16:10.000Z}\n" +
                "Interval{lo=2015-02-23T10:30:55.000Z, hi=2015-02-23T10:31:10.000Z}\n" +
                "Interval{lo=2015-02-23T10:45:55.000Z, hi=2015-02-23T10:46:10.000Z}\n" +
                "Interval{lo=2015-02-23T11:00:55.000Z, hi=2015-02-23T11:01:10.000Z}\n";

        StringBuilder b = new StringBuilder();
        for (Interval in : m.intervalSource) {
            b.append(in.toString());
            b.append('\n');
        }

        Assert.assertEquals(expected, b.toString());
    }

    @Test
    public void testIntervalSourceMonth() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;2h;2M;3'");
        Assert.assertNotNull(m.intervalSource);

        final String expected = "Interval{lo=2015-02-23T10:00:55.000Z, hi=2015-02-23T12:00:55.000Z}\n" +
                "Interval{lo=2015-04-23T10:00:55.000Z, hi=2015-04-23T12:00:55.000Z}\n" +
                "Interval{lo=2015-06-23T10:00:55.000Z, hi=2015-06-23T12:00:55.000Z}\n";

        StringBuilder b = new StringBuilder();
        for (Interval in : m.intervalSource) {
            b.append(in.toString());
            b.append('\n');
        }

        Assert.assertEquals(expected, b.toString());
    }

    @Test
    public void testIntervalSourceSec() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;5s;30s;5'");
        Assert.assertNotNull(m.intervalSource);

        final String expected = "Interval{lo=2015-02-23T10:00:55.000Z, hi=2015-02-23T10:01:00.000Z}\n" +
                "Interval{lo=2015-02-23T10:01:25.000Z, hi=2015-02-23T10:01:30.000Z}\n" +
                "Interval{lo=2015-02-23T10:01:55.000Z, hi=2015-02-23T10:02:00.000Z}\n" +
                "Interval{lo=2015-02-23T10:02:25.000Z, hi=2015-02-23T10:02:30.000Z}\n" +
                "Interval{lo=2015-02-23T10:02:55.000Z, hi=2015-02-23T10:03:00.000Z}\n";

        StringBuilder b = new StringBuilder();
        for (Interval in : m.intervalSource) {
            b.append(in.toString());
            b.append('\n');
        }

        Assert.assertEquals(expected, b.toString());
    }

    @Test
    public void testIntervalSourceYear() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;1d;1y;5'");
        Assert.assertNotNull(m.intervalSource);

        final String expected = "Interval{lo=2015-02-23T10:00:55.000Z, hi=2015-02-24T10:00:55.000Z}\n" +
                "Interval{lo=2016-02-24T10:00:55.000Z, hi=2016-02-25T10:00:55.000Z}\n" +
                "Interval{lo=2017-02-23T10:00:55.000Z, hi=2017-02-24T10:00:55.000Z}\n" +
                "Interval{lo=2018-02-23T10:00:55.000Z, hi=2018-02-24T10:00:55.000Z}\n" +
                "Interval{lo=2019-02-23T10:00:55.000Z, hi=2019-02-24T10:00:55.000Z}\n";

        StringBuilder b = new StringBuilder();
        for (Interval in : m.intervalSource) {
            b.append(in.toString());
            b.append('\n');
        }

        Assert.assertEquals(expected, b.toString());
    }

    @Test
    public void testIntervalTooFewArgs() throws Exception {
        try {
            modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\")");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(e.getMessage().contains("Too few arg"));
        }
    }

    @Test
    public void testIntervalTooFewArgs2() throws Exception {
        try {
            modelOf("timestamp in ()");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(e.getMessage().contains("Too few arg"));
        }
    }

    @Test
    public void testIntervalTooManyArgs() throws Exception {
        try {
            modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\", \"2014-01-03T12:30:00.000Z\")");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(e.getMessage().contains("Too many arg"));
        }
    }

    @Test
    public void testIntrinsicPickup() throws Exception {
        assertFilter(modelOf("timestamp = '2014-06-20T13:25:00.000Z;10m;2d;4' and sym in ('A', 'B') or ex = 'D'"), "'D'ex='B''A'symin'2014-06-20T13:25:00.000Z;10m;2d;4'timestamp=andor");
        assertFilter(modelOf("timestamp = '2014-06-20T13:25:00.000Z;10m;2d;4' or ex = 'D' and sym in ('A', 'B')"), "'D'ex='2014-06-20T13:25:00.000Z;10m;2d;4'timestamp=or");
    }

    @Test(expected = ParserException.class)
    public void testInvalidIntervalSource1() throws Exception {
        modelOf("timestamp = '2014-06-20T13:25:00.000Z;10m;2d'");
    }

    @Test(expected = ParserException.class)
    public void testInvalidIntervalSource2() throws Exception {
        modelOf("timestamp = '2014-06-20T13:25:00.000Z;10m;2d;4;4'");
    }

    @Test
    public void testListOfValuesNegativeOverlap() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and sym in ('c')");
        Assert.assertEquals(IntrinsicValue.FALSE, m.intrinsicValue);
    }

    @Test
    public void testListOfValuesPositiveOverlap() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and sym in ('z')");
        Assert.assertNull(m.filter);
        Assert.assertEquals(IntrinsicValue.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[z]", m.keyValues.toString());
    }

    @Test
    public void testListOfValuesPositiveOverlapQuoteIndifference() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', \"z\") and sym in ('z')");
        Assert.assertNull(m.filter);
        Assert.assertEquals(IntrinsicValue.UNDEFINED, m.intrinsicValue);
        Assert.assertEquals("[z]", m.keyValues.toString());
    }

    @Test
    public void testLiteralInInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\", c)");
        Assert.assertEquals(Long.MIN_VALUE, m.intervalLo);
        Assert.assertEquals(Long.MAX_VALUE, m.intervalHi);
        assertFilter(m, "c\"2014-01-01T12:30:00.000Z\"timestampin");
    }

    @Test
    public void testLiteralInListOfValues() throws Exception {
        IntrinsicModel m = modelOf("sym in (\"a\", z) and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertTrue(m.intervalLo > Long.MIN_VALUE);
        Assert.assertTrue(m.intervalHi < Long.MAX_VALUE);
        Assert.assertNull(m.keyColumn);
        assertFilter(m, "z\"a\"symin");
    }

    @Test
    public void testLiteralInListOfValuesInvalidColumn() throws Exception {
        try {
            modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and x in ('a', z)");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(74, e.getPosition());
        }
    }

    @Test
    public void testManualInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp >= '2014-01-01T15:30:00.000Z' and timestamp < '2014-01-02T12:30:00.000Z'");
        Assert.assertEquals("2014-01-01T15:30:00.000Z", Dates.toString(m.intervalLo));
        Assert.assertEquals("2014-01-02T12:29:59.999Z", Dates.toString(m.intervalHi));
    }

    @Test
    public void testManualIntervalInverted() throws Exception {
        IntrinsicModel m = modelOf("'2014-01-02T12:30:00.000Z' > timestamp and '2014-01-01T15:30:00.000Z' <= timestamp ");
        Assert.assertEquals("2014-01-01T15:30:00.000Z", Dates.toString(m.intervalLo));
        Assert.assertEquals("2014-01-02T12:29:59.999Z", Dates.toString(m.intervalHi));
    }

    @Test
    public void testMultipleAnds() throws Exception {
        IntrinsicModel m = modelOf("a > 10 and b > 20 and (c > 100 and d < 20 and bid = 30)");
        assertFilter(m, "30bid=20d<100c>andand20b>10a>andand");
    }

    @Test
    public void testNestedFunctionTest() throws Exception {
        IntrinsicModel m = modelOf("substr(parse(x, 1, 3), 2, 4)");
        Assert.assertEquals(Long.MIN_VALUE, m.intervalLo);
        Assert.assertEquals(Long.MAX_VALUE, m.intervalHi);
        assertFilter(m, "4231xparsesubstr");
    }

    @Test
    public void testNoIntrinsics() throws Exception {
        IntrinsicModel m = modelOf("a > 10 or b > 20");
        Assert.assertEquals(Long.MIN_VALUE, m.intervalLo);
        Assert.assertEquals(Long.MAX_VALUE, m.intervalHi);
        Assert.assertNull(m.keyColumn);
        assertFilter(m, "20b>10a>or");
    }

    @Test
    public void testNonLiteralColumn() throws Exception {
        try {
            modelOf("10 in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(e.getMessage().contains("Column name expected"));
        }
    }

    @Test
    public void testOr() throws Exception {
        modelOf("(sym = 'X' or sym = 'Y') and bid > 10");

    }

    @Test
    public void testPreferredColumn() throws Exception {
        IntrinsicModel m;
        m = modelOf("sym in ('a', 'b') and ex in ('c') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110", "ex");
        assertFilter(m, "110ask<100bid>'b''a'syminandand");
        Assert.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[c]", m.keyValues.toString());
        Assert.assertEquals("2014-01-01T12:30:00.000Z", Dates.toString(m.intervalLo));
        Assert.assertEquals("2014-01-02T12:30:00.000Z", Dates.toString(m.intervalHi));
    }

    @Test
    public void testPreferredColumn2() throws Exception {
        IntrinsicModel m;
        m = modelOf("ex in ('c') and sym in ('a', 'b') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110", "ex");
        assertFilter(m, "110ask<100bid>'b''a'syminandand");
        Assert.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[c]", m.keyValues.toString());
        Assert.assertEquals("2014-01-01T12:30:00.000Z", Dates.toString(m.intervalLo));
        Assert.assertEquals("2014-01-02T12:30:00.000Z", Dates.toString(m.intervalHi));
    }

    @Test
    public void testPreferredColumn3() throws Exception {
        IntrinsicModel m;
        m = modelOf("sym in ('a', 'b') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110", "ex");
        assertFilter(m, "110ask<100bid>'b''a'syminandand");
        Assert.assertNull(m.keyColumn);
        Assert.assertEquals("2014-01-01T12:30:00.000Z", Dates.toString(m.intervalLo));
        Assert.assertEquals("2014-01-02T12:30:00.000Z", Dates.toString(m.intervalHi));
    }

    @Test
    public void testSimpleInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertTrue(m.intervalLo > Long.MIN_VALUE);
        Assert.assertTrue(m.intervalHi < Long.MAX_VALUE);
        Assert.assertTrue(m.intervalLo < m.intervalHi);
        Assert.assertNull(m.filter);
    }

    @Test
    public void testSingleQuoteInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        Assert.assertTrue(m.intervalLo > Long.MIN_VALUE);
        Assert.assertTrue(m.intervalHi < Long.MAX_VALUE);
        Assert.assertTrue(m.intervalLo < m.intervalHi);
        Assert.assertNull(m.filter);
    }

    @Test
    public void testThreeIntrinsics() throws Exception {
        IntrinsicModel m;
        m = modelOf("sym in ('a', 'b') and ex in ('c') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110");
        assertFilter(m, "110ask<100bid>'c'exinandand");
        Assert.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[a,b]", m.keyValues.toString());
        Assert.assertEquals("2014-01-01T12:30:00.000Z", Dates.toString(m.intervalLo));
        Assert.assertEquals("2014-01-02T12:30:00.000Z", Dates.toString(m.intervalHi));
    }

    @Test
    public void testThreeIntrinsics2() throws Exception {
        IntrinsicModel m;
        m = modelOf("ex in ('c') and sym in ('a', 'b') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110");
        assertFilter(m, "110ask<100bid>'c'exinandand");
        Assert.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[a,b]", m.keyValues.toString());
        Assert.assertEquals("2014-01-01T12:30:00.000Z", Dates.toString(m.intervalLo));
        Assert.assertEquals("2014-01-02T12:30:00.000Z", Dates.toString(m.intervalHi));
    }

    @Test
    public void testTwoExactMatchDifferentDates() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-11T15:03:10.000Z' and timestamp = '2015-05-11'");
        assertFilter(m, "'2015-05-11'timestamp=");
        Assert.assertEquals("2015-05-10T15:03:10.000Z", Dates.toString(m.millis));
        Assert.assertEquals(Long.MIN_VALUE, m.intervalLo);
        Assert.assertEquals(Long.MAX_VALUE, m.intervalHi);
        Assert.assertNull(m.intervalSource);
        Assert.assertEquals(IntrinsicValue.FALSE, m.intrinsicValue);
    }

    @Test
    public void testTwoExactSameDates() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-11'");
        assertFilter(m, "'2015-05-11'timestamp=");
        Assert.assertEquals("2015-05-10T15:03:10.000Z", Dates.toString(m.millis));
        Assert.assertEquals(Long.MIN_VALUE, m.intervalLo);
        Assert.assertEquals(Long.MAX_VALUE, m.intervalHi);
        Assert.assertNull(m.intervalSource);
        Assert.assertEquals(IntrinsicValue.UNDEFINED, m.intrinsicValue);
    }

    @Test(expected = ParserException.class)
    public void testTwoIntervalSources() throws Exception {
        modelOf("timestamp = '2014-06-20T13:25:00.000Z;10m;2d;5' and timestamp = '2015-06-20T13:25:00.000Z;10m;2d;5'");
    }

    @Test
    public void testTwoIntervals() throws Exception {
        IntrinsicModel m = modelOf("bid > 100 and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\") and timestamp in (\"2014-01-01T16:30:00.000Z\", \"2014-01-05T12:30:00.000Z\")");
        Assert.assertTrue(m.intervalLo > Long.MIN_VALUE);
        Assert.assertTrue(m.intervalHi < Long.MAX_VALUE);
        Assert.assertEquals("2014-01-01T16:30:00.000Z", Dates.toString(m.intervalLo));
        Assert.assertEquals("2014-01-02T12:30:00.000Z", Dates.toString(m.intervalHi));
    }

    private void assertFilter(IntrinsicModel m, CharSequence expected) throws ParserException {
        Assert.assertNotNull(m.filter);
        TestUtils.assertEquals(expected, toRpn(m.filter));
    }

    private IntrinsicModel modelOf(CharSequence seq) throws ParserException {
        return modelOf(seq, null);
    }

    private IntrinsicModel modelOf(CharSequence seq, String preferredColumn) throws ParserException {
        p.parseExpr(seq, ast);
        return e.extract(ast.root(), w.getMetadata(), preferredColumn);
    }

    private CharSequence toRpn(ExprNode node) throws ParserException {
        rpn.reset();
        traversalAlgo.traverse(node, rpnBuilderVisitor);
        return rpn.rpn();
    }
}
