/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.parser.sql;

import com.questdb.ex.ParserException;
import com.questdb.model.Quote;
import com.questdb.parser.sql.model.ExprNode;
import com.questdb.parser.sql.model.IntrinsicModel;
import com.questdb.parser.sql.model.IntrinsicValue;
import com.questdb.std.Chars;
import com.questdb.std.Lexer;
import com.questdb.std.ObjectPool;
import com.questdb.store.JournalWriter;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryFilterAnalyserTest extends AbstractTest {

    private final RpnBuilder rpn = new RpnBuilder();
    private final ObjectPool<ExprNode> exprNodeObjectPool = new ObjectPool<>(ExprNode.FACTORY, 128);
    private final Lexer lexer = new Lexer();
    private final ExprParser p = new ExprParser(exprNodeObjectPool);
    private final ExprAstBuilder ast = new ExprAstBuilder();
    private final QueryFilterAnalyser e = new QueryFilterAnalyser();
    private final PostOrderTreeTraversalAlgo traversalAlgo = new PostOrderTreeTraversalAlgo();
    private final PostOrderTreeTraversalAlgo.Visitor rpnBuilderVisitor = rpn::onNode;
    private JournalWriter<Quote> w;

    @Before
    public void setUp() throws Exception {
        w = getFactory().writer(Quote.class);
        exprNodeObjectPool.clear();
        ExprParser.configureLexer(lexer);
    }

    @After
    public void tearDown() {
        w.close();
    }

    @Test
    public void testAndBranchWithNonIndexedField() throws Exception {
        IntrinsicModel m = modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\") and bid > 100");
        Assert.assertEquals("[Interval{lo=2014-01-01T12:30:00.000Z, hi=2014-01-02T12:30:00.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
        assertFilter(m, "100bid>");
    }

    @Test
    public void testBadCountInInterval() {
        try {
            modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m;10;z'");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(12, QueryError.getPosition());
        }
    }

    @Test
    public void testBadDate() {
        try {
            modelOf("timestamp = '2015-02-23T10:00:55.00z;30m'");
            Assert.fail();
        } catch (ParserException e1) {
            Assert.assertEquals(12, QueryError.getPosition());
        }
    }

    @Test
    public void testBadDateInGreater() {
        try {
            modelOf("'2014-0x-01T12:30:00.000Z' > timestamp");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(0, QueryError.getPosition());
        }
    }

    @Test
    public void testBadDateInGreater2() {
        try {
            modelOf("timestamp > '2014-0x-01T12:30:00.000Z'");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(12, QueryError.getPosition());
        }
    }

    @Test
    public void testBadDateInInterval() {
        try {
            modelOf("timestamp = '2014-0x-01T12:30:00.000Z'");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(12, QueryError.getPosition());
        }
    }

    @Test
    public void testBadDateInLess1() {
        try {
            modelOf("timestamp < '2014-0x-01T12:30:00.000Z'");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(12, QueryError.getPosition());
        }
    }

    @Test
    public void testBadDateInLess2() {
        try {
            modelOf("'2014-0x-01T12:30:00.000Z' < timestamp");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(0, QueryError.getPosition());
        }
    }

    @Test
    public void testBadEndDate() {
        try {
            modelOf("timestamp in (\"2014-01-02T12:30:00.000Z\", \"2014-01Z\")");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Unknown date format"));
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
        } catch (ParserException e) {
            Assert.assertEquals(12, QueryError.getPosition());
        }
    }

    @Test
    public void testBadPeriodInInterval2() {
        try {
            modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m;10x;5'");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(12, QueryError.getPosition());
        }
    }

    @Test
    public void testBadRangeInInterval() {
        try {
            modelOf("timestamp = '2014-03-01T12:30:00.000Z;x'");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(12, QueryError.getPosition());
        }
    }

    @Test
    public void testBadStartDate() {
        try {
            modelOf("timestamp in (\"2014-01Z\", \"2014-01-02T12:30:00.000Z\")");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Unknown date format"));
        }
    }

    @Test
    public void testComplexInterval1() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00;2d'");
        Assert.assertEquals("[Interval{lo=2015-02-23T10:00:00.000Z, hi=2015-02-25T10:00:59.999Z}]", IntervalCompiler.asIntervalStr(m.intervals));
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', filter=null}", m.toString());
    }

    @Test
    public void testComplexInterval2() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;7d'");
        Assert.assertEquals("[Interval{lo=2015-02-23T10:00:55.000Z, hi=2015-03-02T10:00:55.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', filter=null}", m.toString());
    }

    @Test
    public void testComplexInterval3() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;15s'");
        Assert.assertEquals("[Interval{lo=2015-02-23T10:00:55.000Z, hi=2015-02-23T10:01:10.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', filter=null}", m.toString());
    }

    @Test
    public void testComplexInterval4() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m'");
        Assert.assertEquals("[Interval{lo=2015-02-23T10:00:55.000Z, hi=2015-02-23T10:30:55.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', filter=null}", m.toString());
    }

    @Test
    public void testComplexInterval5() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m' and timestamp != '2015-02-23T10:10:00.000Z'");
        Assert.assertEquals("[Interval{lo=2015-02-23T10:00:55.000Z, hi=2015-02-23T10:09:59.999Z},Interval{lo=2015-02-23T10:10:00.001Z, hi=2015-02-23T10:30:55.000Z}]",
                IntervalCompiler.asIntervalStr(m.intervals));
        Assert.assertEquals("IntrinsicModel{keyValues=[], keyColumn='null', filter=null}", m.toString());
    }

    @Test
    public void testConstVsLambda() throws Exception {
        IntrinsicModel m = modelOf("ex in (1,2) and sym in (`xyz`)");
        Assert.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(1, m.keyValues.size());
        Assert.assertEquals("xyz", m.keyValues.get(0));
        Assert.assertTrue(m.keyValuesIsLambda);
        Assert.assertNotNull(m.filter);
        Assert.assertEquals("ex12in", TestUtils.toRpn(m.filter));
    }

    @Test
    public void testConstVsLambda2() throws Exception {
        IntrinsicModel m = modelOf("sym in (1,2) and sym in (`xyz`)");
        Assert.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(1, m.keyValues.size());
        Assert.assertEquals("xyz", m.keyValues.get(0));
        Assert.assertTrue(m.keyValuesIsLambda);
        Assert.assertNotNull(m.filter);
        Assert.assertEquals("sym12in", TestUtils.toRpn(m.filter));
    }

    @Test
    public void testContradictingNullSearch() throws Exception {
        IntrinsicModel m = modelOf("sym = null and sym != null and ex != 'blah'");
        Assert.assertEquals(IntrinsicValue.FALSE, m.intrinsicValue);
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
        Assert.assertEquals(IntrinsicValue.FALSE, m.intrinsicValue);
    }

    @Test
    public void testDubiousLess() throws Exception {
        IntrinsicModel m = modelOf("ts < ts");
        Assert.assertEquals(IntrinsicValue.FALSE, m.intrinsicValue);
    }

    @Test
    public void testDubiousNotEquals() throws Exception {
        IntrinsicModel m = modelOf("ts != ts");
        Assert.assertEquals(IntrinsicValue.FALSE, m.intrinsicValue);
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
    public void testEqualsInvalidColumn() {
        try {
            modelOf("sym = 'X' and x = 'Y'");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(14, QueryError.getPosition());
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
        Assert.assertEquals("[12]", m.keyValuePositions.toString());
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
        Assert.assertEquals("[Interval{lo=2015-05-10T15:03:10.000Z, hi=2015-05-10T15:03:10.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testExactDateVsInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-11'");
        Assert.assertEquals(IntrinsicValue.FALSE, m.intrinsicValue);
        Assert.assertNull(m.filter);
    }

    @Test
    public void testFilterAndInterval() throws Exception {
        IntrinsicModel m = modelOf("bid > 100 and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertEquals("[Interval{lo=2014-01-01T12:30:00.000Z, hi=2014-01-02T12:30:00.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
        assertFilter(m, "100bid>");
    }

    @Test
    public void testFilterMultipleKeysAndInterval() throws Exception {
        IntrinsicModel m = modelOf("sym in (\"a\", \"b\", \"c\") and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertEquals("[Interval{lo=2014-01-01T12:30:00.000Z, hi=2014-01-02T12:30:00.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
        Assert.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[a,b,c]", m.keyValues.toString());
        Assert.assertEquals("[8,13,18]", m.keyValuePositions.toString());
        Assert.assertNull(m.filter);
    }

    @Test
    public void testFilterOnIndexedFieldAndInterval() throws Exception {
        IntrinsicModel m = modelOf("sym in ('a') and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertEquals("[Interval{lo=2014-01-01T12:30:00.000Z, hi=2014-01-02T12:30:00.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
        Assert.assertEquals("sym", m.keyColumn);
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
        Assert.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[X,null,Y]", m.keyValues.toString());
    }

    @Test
    public void testInVsEqualInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and timestamp = '2014-01-01'");
        Assert.assertNull(m.filter);
        Assert.assertEquals("[Interval{lo=2014-01-01T12:30:00.000Z, hi=2014-01-01T23:59:59.999Z}]", IntervalCompiler.asIntervalStr(m.intervals));
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
        } catch (ParserException e) {
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Too few arguments"));
        }
    }

    @Test
    public void testIntervalGreater1() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and timestamp > '2014-01-01T15:30:00.000Z'");
        Assert.assertEquals("[Interval{lo=2014-01-01T15:30:00.001Z, hi=2014-01-02T12:30:00.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testIntervalGreater2() throws Exception {
        IntrinsicModel m = modelOf("timestamp > '2014-01-01T15:30:00.000Z' and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        Assert.assertEquals("[Interval{lo=2014-01-01T15:30:00.001Z, hi=2014-01-02T12:30:00.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testIntervalGreaterOrEq1() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and timestamp >= '2014-01-01T15:30:00.000Z'");
        Assert.assertEquals("[Interval{lo=2014-01-01T15:30:00.000Z, hi=2014-01-02T12:30:00.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testIntervalGreaterOrEq2() throws Exception {
        IntrinsicModel m = modelOf("timestamp >= '2014-01-01T15:30:00.000Z' and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        Assert.assertEquals("[Interval{lo=2014-01-01T15:30:00.000Z, hi=2014-01-02T12:30:00.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testIntervalSourceDay() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;30m;2d;5'");
        Assert.assertEquals("[Interval{lo=2015-02-23T10:00:55.000Z, hi=2015-02-23T10:30:55.000Z},Interval{lo=2015-02-25T10:00:55.000Z, hi=2015-02-25T10:30:55.000Z},Interval{lo=2015-02-27T10:00:55.000Z, hi=2015-02-27T10:30:55.000Z},Interval{lo=2015-03-01T10:00:55.000Z, hi=2015-03-01T10:30:55.000Z},Interval{lo=2015-03-03T10:00:55.000Z, hi=2015-03-03T10:30:55.000Z}]",
                IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testIntervalSourceHour() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;10m;3h;10'");
        final String expected = "[Interval{lo=2015-02-23T10:00:55.000Z, hi=2015-02-23T10:10:55.000Z}," +
                "Interval{lo=2015-02-23T13:00:55.000Z, hi=2015-02-23T13:10:55.000Z}," +
                "Interval{lo=2015-02-23T16:00:55.000Z, hi=2015-02-23T16:10:55.000Z}," +
                "Interval{lo=2015-02-23T19:00:55.000Z, hi=2015-02-23T19:10:55.000Z}," +
                "Interval{lo=2015-02-23T22:00:55.000Z, hi=2015-02-23T22:10:55.000Z}," +
                "Interval{lo=2015-02-24T01:00:55.000Z, hi=2015-02-24T01:10:55.000Z}," +
                "Interval{lo=2015-02-24T04:00:55.000Z, hi=2015-02-24T04:10:55.000Z}," +
                "Interval{lo=2015-02-24T07:00:55.000Z, hi=2015-02-24T07:10:55.000Z}," +
                "Interval{lo=2015-02-24T10:00:55.000Z, hi=2015-02-24T10:10:55.000Z}," +
                "Interval{lo=2015-02-24T13:00:55.000Z, hi=2015-02-24T13:10:55.000Z}]";

        Assert.assertEquals(expected, IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testIntervalSourceMin() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;15s;15m;5'");
        final String expected = "[Interval{lo=2015-02-23T10:00:55.000Z, hi=2015-02-23T10:01:10.000Z}," +
                "Interval{lo=2015-02-23T10:15:55.000Z, hi=2015-02-23T10:16:10.000Z}," +
                "Interval{lo=2015-02-23T10:30:55.000Z, hi=2015-02-23T10:31:10.000Z}," +
                "Interval{lo=2015-02-23T10:45:55.000Z, hi=2015-02-23T10:46:10.000Z}," +
                "Interval{lo=2015-02-23T11:00:55.000Z, hi=2015-02-23T11:01:10.000Z}]";
        Assert.assertEquals(expected, IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testIntervalSourceMonth() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;2h;2M;3'");
        final String expected = "[Interval{lo=2015-02-23T10:00:55.000Z, hi=2015-02-23T12:00:55.000Z}," +
                "Interval{lo=2015-04-23T10:00:55.000Z, hi=2015-04-23T12:00:55.000Z}," +
                "Interval{lo=2015-06-23T10:00:55.000Z, hi=2015-06-23T12:00:55.000Z}]";

        Assert.assertEquals(expected, IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testIntervalSourceSec() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;5s;30s;5'");
        final String expected = "[Interval{lo=2015-02-23T10:00:55.000Z, hi=2015-02-23T10:01:00.000Z}," +
                "Interval{lo=2015-02-23T10:01:25.000Z, hi=2015-02-23T10:01:30.000Z}," +
                "Interval{lo=2015-02-23T10:01:55.000Z, hi=2015-02-23T10:02:00.000Z}," +
                "Interval{lo=2015-02-23T10:02:25.000Z, hi=2015-02-23T10:02:30.000Z}," +
                "Interval{lo=2015-02-23T10:02:55.000Z, hi=2015-02-23T10:03:00.000Z}]";

        Assert.assertEquals(expected, IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testIntervalSourceYear() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-02-23T10:00:55.000Z;1d;1y;5'");
        final String expected = "[Interval{lo=2015-02-23T10:00:55.000Z, hi=2015-02-24T10:00:55.000Z}," +
                "Interval{lo=2016-02-23T10:00:55.000Z, hi=2016-02-24T10:00:55.000Z}," +
                "Interval{lo=2017-02-23T10:00:55.000Z, hi=2017-02-24T10:00:55.000Z}," +
                "Interval{lo=2018-02-23T10:00:55.000Z, hi=2018-02-24T10:00:55.000Z}," +
                "Interval{lo=2019-02-23T10:00:55.000Z, hi=2019-02-24T10:00:55.000Z}]";

        Assert.assertEquals(expected, IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testIntervalTooFewArgs() {
        try {
            modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\")");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Too few arg"));
        }
    }

    @Test
    public void testIntervalTooFewArgs2() {
        try {
            modelOf("timestamp in ()");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Too few arg"));
        }
    }

    @Test
    public void testIntervalTooManyArgs() {
        try {
            modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\", \"2014-01-03T12:30:00.000Z\")");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Too many arg"));
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
    public void testLambdaVsConst() throws Exception {
        IntrinsicModel m = modelOf("sym in (`xyz`) and ex in (1,2)");
        Assert.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(1, m.keyValues.size());
        Assert.assertEquals("xyz", m.keyValues.get(0));
        Assert.assertTrue(m.keyValuesIsLambda);
        Assert.assertNotNull(m.filter);
        Assert.assertEquals("ex12in", TestUtils.toRpn(m.filter));
    }

    @Test
    public void testListOfValuesNegativeOverlap() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and sym in ('c')");
        Assert.assertEquals(IntrinsicValue.FALSE, m.intrinsicValue);
    }

    @Test
    public void testListOfValuesOverlapWithNotClause() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and not (sym in ('c', 'd', 'e'))");
        Assert.assertEquals("[a,z]", m.keyValues.toString());
        Assert.assertNull(m.filter);
        Assert.assertEquals(IntrinsicValue.UNDEFINED, m.intrinsicValue);
    }

    @Test
    public void testListOfValuesOverlapWithNotClause2() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and sym in ('a', 'z') and not (sym in ('a', 'd', 'e'))");
        Assert.assertNull(m.filter);
        Assert.assertEquals("[z]", m.keyValues.toString());
        Assert.assertEquals(IntrinsicValue.UNDEFINED, m.intrinsicValue);
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
        Assert.assertNull(m.intervals);
        assertFilter(m, "c\"2014-01-01T12:30:00.000Z\"timestampin");
    }

    @Test
    public void testLiteralInListOfValues() throws Exception {
        IntrinsicModel m = modelOf("sym in (\"a\", z) and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertEquals("[Interval{lo=2014-01-01T12:30:00.000Z, hi=2014-01-02T12:30:00.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
        Assert.assertNull(m.keyColumn);
        assertFilter(m, "z\"a\"symin");
    }

    @Test
    public void testLiteralInListOfValuesInvalidColumn() {
        try {
            modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and x in ('a', z)");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(74, QueryError.getPosition());
        }
    }

    @Test
    public void testManualInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp >= '2014-01-01T15:30:00.000Z' and timestamp < '2014-01-02T12:30:00.000Z'");
        Assert.assertEquals("[Interval{lo=2014-01-01T15:30:00.000Z, hi=2014-01-02T12:29:59.999Z}]", IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testManualIntervalInverted() throws Exception {
        IntrinsicModel m = modelOf("'2014-01-02T12:30:00.000Z' > timestamp and '2014-01-01T15:30:00.000Z' <= timestamp ");
        Assert.assertEquals("[Interval{lo=2014-01-01T15:30:00.000Z, hi=2014-01-02T12:29:59.999Z}]", IntervalCompiler.asIntervalStr(m.intervals));
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
    public void testNonLiteralColumn() {
        try {
            modelOf("10 in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Column name expected"));
        }
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
    public void testOr() throws Exception {
        IntrinsicModel m = modelOf("(sym = 'X' or sym = 'Y') and bid > 10");
        Assert.assertEquals(IntrinsicValue.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "10bid>'Y'sym='X'sym=orand");
    }

    @Test
    public void testOrNullSearch() throws Exception {
        IntrinsicModel m = modelOf("sym = null or sym != null and ex != 'blah'");
        Assert.assertEquals(IntrinsicValue.UNDEFINED, m.intrinsicValue);
        assertFilter(m, "'blah'ex!=nullsym!=nullsym=orand");
        Assert.assertEquals("[]", m.keyValues.toString());
        Assert.assertEquals("[]", m.keyValuePositions.toString());
    }

    @Test
    public void testPreferredColumn() throws Exception {
        IntrinsicModel m;
        m = modelOf("sym in ('a', 'b') and ex in ('c') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110", "ex");
        assertFilter(m, "110ask<100bid>'b''a'syminandand");
        Assert.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[c]", m.keyValues.toString());
        Assert.assertEquals("[Interval{lo=2014-01-01T12:30:00.000Z, hi=2014-01-02T12:30:00.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testPreferredColumn2() throws Exception {
        IntrinsicModel m;
        m = modelOf("ex in ('c') and sym in ('a', 'b') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110", "ex");
        assertFilter(m, "110ask<100bid>'b''a'syminandand");
        Assert.assertEquals("ex", m.keyColumn);
        Assert.assertEquals("[c]", m.keyValues.toString());
        Assert.assertEquals("[Interval{lo=2014-01-01T12:30:00.000Z, hi=2014-01-02T12:30:00.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testPreferredColumn3() throws Exception {
        IntrinsicModel m;
        m = modelOf("sym in ('a', 'b') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110", "ex");
        assertFilter(m, "110ask<100bid>'b''a'syminandand");
        Assert.assertNull(m.keyColumn);
        Assert.assertEquals("[Interval{lo=2014-01-01T12:30:00.000Z, hi=2014-01-02T12:30:00.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testSimpleInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\")");
        Assert.assertEquals("[Interval{lo=2014-01-01T12:30:00.000Z, hi=2014-01-02T12:30:00.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testSimpleLambda() throws Exception {
        IntrinsicModel m = modelOf("sym in (`xyz`)");
        Assert.assertEquals("xyz", m.keyValues.get(0));
        Assert.assertTrue(m.keyValuesIsLambda);
    }

    @Test
    public void testSingleQuoteInterval() throws Exception {
        IntrinsicModel m = modelOf("timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z')");
        Assert.assertEquals("[Interval{lo=2014-01-01T12:30:00.000Z, hi=2014-01-02T12:30:00.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
        Assert.assertNull(m.filter);
    }

    @Test
    public void testThreeIntrinsics() throws Exception {
        IntrinsicModel m;
        m = modelOf("sym in ('a', 'b') and ex in ('c') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110");
        assertFilter(m, "110ask<100bid>'c'exinandand");
        Assert.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[a,b]", m.keyValues.toString());
        Assert.assertEquals("[Interval{lo=2014-01-01T12:30:00.000Z, hi=2014-01-02T12:30:00.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testThreeIntrinsics2() throws Exception {
        IntrinsicModel m;
        m = modelOf("ex in ('c') and sym in ('a', 'b') and timestamp in ('2014-01-01T12:30:00.000Z', '2014-01-02T12:30:00.000Z') and bid > 100 and ask < 110");
        assertFilter(m, "110ask<100bid>'c'exinandand");
        Assert.assertEquals("sym", m.keyColumn);
        Assert.assertEquals("[a,b]", m.keyValues.toString());
        Assert.assertEquals("[Interval{lo=2014-01-01T12:30:00.000Z, hi=2014-01-02T12:30:00.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testTwoDiffColLambdas() throws Exception {
        IntrinsicModel m = modelOf("sym in (`xyz`) and ex in (`kkk`)");
        Assert.assertEquals("sym", m.keyColumn);
        Assert.assertEquals(1, m.keyValues.size());
        Assert.assertEquals("xyz", m.keyValues.get(0));
        Assert.assertTrue(m.keyValuesIsLambda);
        Assert.assertNotNull(m.filter);
        Assert.assertEquals(ExprNode.LAMBDA, m.filter.rhs.type);
    }

    @Test
    public void testTwoExactMatchDifferentDates() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-11T15:03:10.000Z' and timestamp = '2015-05-11'");
        Assert.assertEquals("[]", IntervalCompiler.asIntervalStr(m.intervals));
        Assert.assertNull(m.filter);
        Assert.assertEquals(IntrinsicValue.FALSE, m.intrinsicValue);
    }

    @Test
    public void testTwoExactSameDates() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-10T15:03:10.000Z' and timestamp = '2015-05-11'");
        Assert.assertEquals("[]", IntervalCompiler.asIntervalStr(m.intervals));
        Assert.assertNull(m.filter);
        Assert.assertEquals(IntrinsicValue.FALSE, m.intrinsicValue);
    }

    @Test
    public void testTwoIntervalSources() throws Exception {
        IntrinsicModel m = modelOf("timestamp = '2014-06-20T13:25:00.000Z;10m;2d;5' and timestamp = '2015-06-20T13:25:00.000Z;10m;2d;5'");
        Assert.assertEquals("[]", IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testTwoIntervals() throws Exception {
        IntrinsicModel m = modelOf("bid > 100 and timestamp in (\"2014-01-01T12:30:00.000Z\", \"2014-01-02T12:30:00.000Z\") and timestamp in (\"2014-01-01T16:30:00.000Z\", \"2014-01-05T12:30:00.000Z\")");
        Assert.assertEquals("[Interval{lo=2014-01-01T16:30:00.000Z, hi=2014-01-02T12:30:00.000Z}]", IntervalCompiler.asIntervalStr(m.intervals));
    }

    @Test
    public void testTwoSameColLambdas() {
        try {
            modelOf("sym in (`xyz`) and sym in (`kkk`)");
            Assert.fail("exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(4, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Multiple lambda"));
        }
    }

    private void assertFilter(IntrinsicModel m, CharSequence expected) throws ParserException {
        Assert.assertNotNull(m.filter);
        TestUtils.assertEquals(expected, toRpn(m.filter));
    }

    private IntrinsicModel modelOf(CharSequence seq) throws ParserException {
        return modelOf(seq, null);
    }

    private IntrinsicModel modelOf(CharSequence seq, String preferredColumn) throws ParserException {
        lexer.setContent(seq);
        p.parseExpr(lexer, ast);
        return e.extract(column -> column, ast.poll(), w.getMetadata(), preferredColumn, w.getMetadata().getTimestampIndex());
    }

    private void testBadOperator(String op) {
        try {
            modelOf("sum(ts) " + op);
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(8, QueryError.getPosition());
        }

        try {
            modelOf(op + " sum(ts)");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(0, QueryError.getPosition());
        }

    }

    private CharSequence toRpn(ExprNode node) throws ParserException {
        rpn.reset();
        traversalAlgo.traverse(node, rpnBuilderVisitor);
        return rpn.rpn();
    }
}
